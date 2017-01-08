package index;

import global.Minibase;
import global.PageId;

/**
 * An object in this class is a page in a linked list.
 * The entire linked list is a hash table bucket.
 */
class HashBucketPage extends SortedPage {

	/**
	 * Gets the number of entries in this page and later
	 * (overflow) pages in the list.
	 * <br><br>
	 * To find the number of entries in a bucket, apply 
	 * countEntries to the primary page of the bucket.
	 */
	public int countEntries() {
		int entryCount = getEntryCount();

		PageId nextPageId = getNextPage();

		while(nextPageId.pid != INVALID_PAGEID){

			//pin the next pageId
			HashBucketPage nextPage = new HashBucketPage();
			Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);

			//add the new entry count to the total
			entryCount += nextPage.countEntries();

			//get next page, unpin
			PageId tempPageId = nextPage.getNextPage();
			Minibase.BufferManager.unpinPage(nextPageId, UNPIN_CLEAN);

			//set the next page
			nextPageId.copyPageId(tempPageId);

		}

		return entryCount;

		//throw new UnsupportedOperationException("Not implemented");

	} // public int countEntries()

	/**
	 * Inserts a new data entry into this page. If there is no room
	 * on this page, recursively inserts in later pages of the list.  
	 * If necessary, creates a new page at the end of the list.
	 * Does not worry about keeping order between entries in different pages.
	 * <br><br>
	 * To insert a data entry into a bucket, apply insertEntry to the
	 * primary page of the bucket.
	 * 
	 * @return true if inserting made this page dirty, false otherwise
	 */
	public boolean insertEntry(DataEntry entry) {

		//try to insert into current page, if not enough space,
		//catch the illegal state exception (doesn't return a false)
		try{
			super.insertEntry(entry);
			return true;
		}
		catch (IllegalStateException exc){

			PageId nextPageId = getNextPage();

			//this section executed if a next page is available
			if(nextPageId.pid != INVALID_PAGEID){
				//pin the page
				HashBucketPage nextPage = new HashBucketPage();
				Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);

				//here is the recursive call to insertEntry
				boolean isDirty = nextPage.insertEntry(entry);

				//on return from the recursive call, check if the next page
				//was able to be updated so we know whether to unpin as dirty/clean
				if(isDirty){
					Minibase.BufferManager.unpinPage(nextPageId, UNPIN_DIRTY);
				}
				else{
					Minibase.BufferManager.unpinPage(nextPageId, UNPIN_CLEAN);
				}

				//since the next page is available, caller can get away with unpin clean
				return false;
			}

			//this section executed if a next page is not available
			else{
				//create/set a new page with a run size of 1
				HashBucketPage nextPage = new HashBucketPage();
				nextPageId = Minibase.BufferManager.newPage(nextPage, 1);
				setNextPage(nextPageId);

				//here is the recursive call
				//no need to check if returns true/false because must unpin dirty since changed NextPage
				nextPage.insertEntry(entry);

				Minibase.BufferManager.unpinPage(nextPageId, UNPIN_DIRTY);

				//return true to let caller know to unpin dirty
				return true;

			}

		}

		//throw new UnsupportedOperationException("Not implemented");

	} // public boolean insertEntry(DataEntry entry)

	/**
	 * Deletes a data entry from this page.  If a page in the list 
	 * (not the primary page) becomes empty, it is deleted from the list.
	 * 
	 * To delete a data entry from a bucket, apply deleteEntry to the
	 * primary page of the bucket.
	 * 
	 * @return true if deleting made this page dirty, false otherwise
	 * @throws IllegalArgumentException if the entry is not in the list.
	 */
	public boolean deleteEntry(DataEntry entry) {

		//try to delete the entry from the current page
		//if not in page, catch Illegal argument exception (doesn't return false)
		try{
			super.deleteEntry(entry);
			return true;
		}
		catch (IllegalArgumentException exc){

			PageId nextPageId = getNextPage();

			//this section is executed if there is an available next
			//page to check if the entry is in
			if(nextPageId.pid != INVALID_PAGEID){

				//pin the next page
				HashBucketPage nextPage = new HashBucketPage();
				Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);

				//here is the recursive call
				//keep track of return so we know to unpin nextpage clean/dirty
				boolean isDirty = nextPage.deleteEntry(entry);

				//check the count of the next page and delete if appropriate
				if(nextPage.getEntryCount() <= 0){
					//update the current next page
					//no need to adjust next.next page's prev since not a field in SortedPage
					setNextPage(nextPage.getNextPage());

					//should always be a dirty return if we are deleting, need to free the page
					Minibase.BufferManager.unpinPage(nextPageId, UNPIN_DIRTY);
					Minibase.BufferManager.freePage(nextPageId);

					//early return true to let caller know to unpin dirty due to nextPage update
					return true;
				}

				//if we get here, check if the next page
				//was able to be updated so we know whether to unpin as dirty/clean
				if(isDirty){
					Minibase.BufferManager.unpinPage(nextPageId, UNPIN_DIRTY);
				}
				else{
					Minibase.BufferManager.unpinPage(nextPageId, UNPIN_CLEAN);
				}

				//since the next page wasn't updated, caller can get away with unpin clean
				return false;

			}

			//if we get here, then the entry was not found in any page and unable to delete
			throw new IllegalArgumentException("Entry not found in the HashBucket list");

		}

		// throw new UnsupportedOperationException("Not implemented");

	} // public boolean deleteEntry(DataEntry entry)



} // class HashBucketPage extends SortedPage
