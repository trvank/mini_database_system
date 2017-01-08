package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;

/**
 * <h3>Minibase Hash Index</h3>
 * This unclustered index implements static hashing as described on pages 371 to
 * 373 of the textbook (3rd edition).  The index file is a stored as a heapfile.  
 */
public class HashIndex implements GlobalConst {

	/** File name of the hash index. */
	protected String fileName;

	/** Page id of the directory. */
	protected PageId headId;

	//Log2 of the number of buckets - fixed for this simple index
	protected final int  DEPTH = 7;

	protected boolean isTemp;

	protected final short BUCKETS = 128;
	
	protected HashDirPage hashDirPage;


	// --------------------------------------------------------------------------

	/**
	 * Opens an index file given its name, or creates a new index file if the name
	 * doesn't exist; a null name produces a temporary index file which requires
	 * no file library entry and whose pages are freed when there are no more
	 * references to it.
	 * The file's directory contains the locations of the 128 primary bucket pages.
	 * You will need to decide on a structure for the directory.
	 * The library entry contains the name of the index file and the pageId of the
	 * file's directory.
	 */
	public HashIndex(String fileName) {

		hashDirPage = new HashDirPage();
		boolean doesExist = false;
		isTemp = false;

		this.fileName = fileName;

		//check if the file is null
		if(fileName == null){
			isTemp = true;
		}

		//if not temp file, try to assign headID to first page
		//and check if the headID is null (not exist)
		if(!isTemp){
			headId = Minibase.DiskManager.get_file_entry(fileName);

			if(headId != null){
				doesExist = true;
			}
		}

		//if the not exist in library, create a new Hash Directory Page
		//and assign returned (pinned) page ID to headId
		//add the file/headId to library
		if(!doesExist){
			headId = Minibase.BufferManager.newPage(hashDirPage, 1);
			Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);

			if(!isTemp){
				Minibase.DiskManager.add_file_entry(fileName, headId);
			}
		}

		//throw new UnsupportedOperationException("Not implemented");

	} // public HashIndex(String fileName)

	/**
	 * Called by the garbage collector when there are no more references to the
	 * object; deletes the index file if it's temporary.
	 */
	protected void finalize() throws Throwable {

		if(isTemp){
			deleteFile();
		}

		//throw new UnsupportedOperationException("Not implemented");

	} // protected void finalize() throws Throwable

	/**
	 * Deletes the index file from the database, freeing all of its pages.
	 */
	public void deleteFile() {

		//the hash bucket page and Id used to pin and freed
		HashBucketPage hashBucketPage = new HashBucketPage();
		PageId hashBucketPageId = new PageId();

		//keep track of next overflow page in hashBucket
		PageId nextHashBucketPageId = new PageId();

		//used to count entries in each page
		//	  int entryCount;

		//pin directory page 
		Minibase.BufferManager.pinPage(headId, hashDirPage, PIN_DISKIO);

		//get the first hash bucket page id for each of the 128 buckets to op on
		for(short slot = 0; slot < BUCKETS; slot++){
			hashBucketPageId.copyPageId(hashDirPage.getPageId(slot));


			//until no more overflow pages, pin HBPage, get the next overflow, and free the page
			while(hashBucketPageId.pid != INVALID_PAGEID){
				Minibase.BufferManager.pinPage(hashBucketPageId, hashBucketPage, PIN_DISKIO);
				nextHashBucketPageId = hashBucketPage.getNextPage();
				Minibase.BufferManager.unpinPage(hashBucketPageId, UNPIN_CLEAN);//clean since getting rid of it
				Minibase.BufferManager.freePage(hashBucketPageId);
				hashBucketPageId.copyPageId(nextHashBucketPageId);
			}
		}

		//all hash buckets removed, so need to free the dir page and delete the file entry
		Minibase.BufferManager.unpinPage(headId, UNPIN_CLEAN);//clean since getting rid of it
		Minibase.DiskManager.delete_file_entry(fileName); 
		
		//throw new UnsupportedOperationException("Not implemented");

	} // public void deleteFile()

	/**
	 * Inserts a new data entry into the index file.
	 * 
	 * @throws IllegalArgumentException if the entry is too large
	 */
	public void insertEntry(SearchKey key, RID rid) {

		//create the data entry and check on its size
		DataEntry dataEntry = new DataEntry(key, rid);

		if(dataEntry.getLength() > SortedPage.MAX_ENTRY_SIZE){
			throw new IllegalArgumentException("data entry is too big!!!");
		}

		//if we get here, the entry is appropriately sized

		//Hash Bucket Page will be needed
		HashBucketPage hashBucketPage = new HashBucketPage();
		PageId hashBucketPageId = new PageId();

		//get the hash value of the search key to determine what bucket it should go in
		int bucket = key.getHash(DEPTH);

		//pin the directory page to access buckets
		Minibase.BufferManager.pinPage(headId, hashDirPage, PIN_DISKIO);

		//determine the selected bucket's pageId
		hashBucketPageId.copyPageId(hashDirPage.getPageId(bucket));

		//check if the bucket already exists, if not, need to make a new page for the bucket
		if(hashBucketPageId.pid != INVALID_PAGEID){
			//exists, so just pin it and the direcory page can be unpinned clean since not being updated
			Minibase.BufferManager.unpinPage(headId, UNPIN_CLEAN);
			Minibase.BufferManager.pinPage(hashBucketPageId, hashBucketPage, PIN_DISKIO);
		}
		else{
			//doesn't exist, so create a new page, add the page id to the directory
			//and unpin the directory as dirty since it was updated
			hashBucketPageId.copyPageId(Minibase.BufferManager.newPage(hashBucketPage, 1));
			hashDirPage.setPageId(bucket, hashBucketPageId);
			Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
		}

		//finally insert the data record in the bucket
		//keep track of return value to see if clean/dirty unpin is needed (might have updated overflow page -->clean)
		boolean isDirty = hashBucketPage.insertEntry(dataEntry);

		if(isDirty){
			Minibase.BufferManager.unpinPage(hashBucketPageId, UNPIN_DIRTY);
		}
		else{
			Minibase.BufferManager.unpinPage(hashBucketPageId, UNPIN_CLEAN);
		}

		//		throw new UnsupportedOperationException("Not implemented");

	} // public void insertEntry(SearchKey key, RID rid)

	/**
	 * Deletes the specified data entry from the index file.
	 * 
	 * @throws IllegalArgumentException if the entry doesn't exist
	 */
	public void deleteEntry(SearchKey key, RID rid) {

		//Hash Bucket Page will be needed
		HashBucketPage hashBucketPage = new HashBucketPage();
		PageId hashBucketPageId = new PageId();

		//create the data entry and the bucket to delete from
		DataEntry dataEntry = new DataEntry(key, rid);
		int bucket = key.getHash(DEPTH);

		//pin the directory page to access buckets
		Minibase.BufferManager.pinPage(headId, hashDirPage, PIN_DISKIO);

		//determine the selected bucket's pageId
		hashBucketPageId.copyPageId(hashDirPage.getPageId(bucket));

		//unpin directory page since won't need any longer
		//since deleting, the directory page won't be updated.
		Minibase.BufferManager.unpinPage(headId, UNPIN_CLEAN);

		if(hashBucketPageId.pid != INVALID_PAGEID){
			//page exists, so pin it and try to delete the entry if it exists in the page
			Minibase.BufferManager.pinPage(hashBucketPageId, hashBucketPage, PIN_DISKIO);

			try{
				//keep track if dirty so know to unpin clean/dirty (clean if update is overflow ex)
				boolean isDirty = hashBucketPage.deleteEntry(dataEntry);
				if(isDirty){
					Minibase.BufferManager.unpinPage(hashBucketPageId, UNPIN_DIRTY);
				}
				else{
					Minibase.BufferManager.unpinPage(hashBucketPageId, UNPIN_CLEAN);
				}
			}
			catch(IllegalArgumentException exc){
				//unpin clean since no update able to be made (entry doesn't exist)
				Minibase.BufferManager.unpinPage(hashBucketPageId, UNPIN_CLEAN);
				throw new IllegalArgumentException("That entry does not exist");
			}
		}

		else{
			//if get here, the bucket is "invalid" so can't contain the entry
			throw new IllegalArgumentException("That entry does not exist");
		}

		//		throw new UnsupportedOperationException("Not implemented");

	} // public void deleteEntry(SearchKey key, RID rid)

	/**
	 * Initiates an equality scan of the index file.
	 */
	public HashScan openScan(SearchKey key) {
		return new HashScan(this, key);
	}

	/**
	 * Returns the name of the index file.
	 */
	public String toString() {
		return fileName;
	}

	/**
	 * Prints a high-level view of the directory, namely which buckets are
	 * allocated and how many entries are stored in each one. Sample output:
	 * 
	 * <pre>
	 * IX_Customers
	 * ------------
	 * 0000000 : 35
	 * 0000001 : null
	 * 0000010 : 27
	 * ...
	 * 1111111 : 42
	 * ------------
	 * Total : 1500
	 * </pre>
	 */
	public void printSummary() {

		//Hash Bucket Page will be needed
		HashBucketPage bucketPage = new HashBucketPage();
		PageId bucketPageId = new PageId();

		//keep track of the count of entries in each bucket and the total
		int entryCount;
		int totalCount = 0;

		//print the header
		System.out.println("\n" + fileName);
		for(int i = 0; i < fileName.length(); i++)
			System.out.print("~");
		System.out.print("\n");

		//pin directory to get each of the bucket pages to get the count for each
		Minibase.BufferManager.pinPage(headId, hashDirPage, PIN_DISKIO);

		for(short slot = 0; slot < BUCKETS; slot++){
			bucketPageId.copyPageId(hashDirPage.getPageId(slot));

			//print the bucket #
			String string = Integer.toBinaryString(slot);
			int zeros = DEPTH - string.length();
			for(int i = 0; i < zeros; i++)
				System.out.print("0");
			System.out.print(string + ": ");

			//if the page is valid, pin it, get the count in the bucket
			//unpin clean (no updates), add to total and print the bucket count
			if(bucketPageId.pid != INVALID_PAGEID){
				Minibase.BufferManager.pinPage(bucketPageId, bucketPage, PIN_DISKIO);
				entryCount = bucketPage.countEntries();
				Minibase.BufferManager.unpinPage(bucketPageId, UNPIN_CLEAN);
				totalCount += entryCount;
				if(entryCount > 0){
					System.out.println(entryCount);
				}
				else{
					System.out.println("NULL");
				}
			}
			else{
				System.out.println("NULL");
			}
		}
		
		//print footer
		for(int i = 0; i < fileName.length(); i++)
			System.out.print("~");
		System.out.println("\nTotal: " + totalCount);

		//throw new UnsupportedOperationException("Not implemented");

	} // public void printSummary()

} // public class HashIndex implements GlobalConst
