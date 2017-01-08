package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;

/**
 * A HashScan retrieves all records with a given key (via the RIDs of the records).  
 * It is created only through the function openScan() in the HashIndex class. 
 */
public class HashScan implements GlobalConst {

  /** The search key to scan for. */
  protected SearchKey key;

  /** Id of HashBucketPage being scanned. */
  protected PageId curPageId;

  /** HashBucketPage being scanned. */
  protected HashBucketPage curPage;

  /** Current slot to scan from. */
  protected int curSlot;
  
  protected final int NOKEY = -1;
  

  // --------------------------------------------------------------------------

  /**
   * Constructs an equality scan by initializing the iterator state.
   */
  protected HashScan(HashIndex index, SearchKey key) {
	  
	  //need the hash directory page to get the appropriate bucket page
	  HashDirPage directoryPage = new HashDirPage();
	  PageId directoryId = new PageId();
	  directoryId.copyPageId(index.headId);
	  
	  //assign the HashScan key value and get the bucket (from hash function)
	  this.key = key; 
	  int bucket = key.getHash(index.DEPTH);
	  
	  //pin the directory
	  Minibase.BufferManager.pinPage(directoryId, directoryPage, PIN_DISKIO);
	  
	  //get bucket's primary pageId and initialize HashScan fields
	  curPageId = new PageId();
	  curPageId.copyPageId(directoryPage.getPageId(bucket));
	  curPage = new HashBucketPage();
	  curSlot = NOKEY; //note: need to initialize curSlot here!!!
	  
	  Minibase.BufferManager.unpinPage(directoryId, UNPIN_CLEAN);
	  
	 // throw new UnsupportedOperationException("Not implemented");

  } // protected HashScan(HashIndex index, SearchKey key)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {
	  
	  if(curPageId.pid != INVALID_PAGEID){
		  close();
	  }

//	  throw new UnsupportedOperationException("Not implemented");

  } // protected void finalize() throws Throwable

  /**
   * Closes the index scan, releasing any pinned pages.
   */
  public void close() {
	  
	  if(curPageId.pid != INVALID_PAGEID){
		  Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
		  curPageId.pid = INVALID_PAGEID;
	  }
	  

//	  throw new UnsupportedOperationException("Not implemented");

  } // public void close()

   /**
   * Gets the next entry's RID in the index scan.
   * 
   * @throws IllegalStateException if the scan has no more entries
   */
  public RID getNext() {
	  //return value
	  RID rid;
	  
	  //will need a "next" pageId to keep track of possible next overflow page
	  PageId next = new PageId();
	  
	  //starting with cur page, check all bucket pages for the key
	  while(curPageId.pid != INVALID_PAGEID){
		  //pin the curPageId
		  Minibase.BufferManager.pinPage(curPageId, curPage, PIN_DISKIO);
		  
		  //Note: sorted page.nextEntry returns slot of the next entry for key or -1 if not found
		  curSlot = curPage.nextEntry(key, curSlot);
		  
		  //if slot not found, check overflow pages (unpin curr and update curPageId and continue loop)
		  if(curSlot == NOKEY){
			  next.copyPageId(curPage.getNextPage());
			  Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
			  curPageId.copyPageId(next);
		  }
		  else{
			  Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
		  }
	  }
	  
	  //if the key is not found, return null
	  if(curSlot != NOKEY){
		  rid = curPage.getEntryAt(curSlot).rid;
		  return rid;
	  }
	  else{
		  return null;
//		  throw new IllegalStateException("no more next entry");
	  }	        

//	  throw new UnsupportedOperationException("Not implemented");

  } // public RID getNext()

} // public class HashScan implements GlobalConst
