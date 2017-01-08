package heap; 

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is the simplest database file structure.  It is an unordered 
 * set of records, stored on a set of data pages. <br>
 * This class supports inserting, selecting, updating, and deleting
 * records.<br>
 * Normally each heap file has an entry in the database's file library.
 * Temporary heap files are used for external sorting and in other
 * relational operators. A temporary heap file does not have an entry in the
 * file library and is deleted when there are no more references to it. <br>
 * A sequential scan of a heap file (via the HeapScan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

  /** HFPage type for directory pages. */
  protected static final short DIR_PAGE = 10;

  /** HFPage type for data pages. */
  protected static final short DATA_PAGE = 11;

  //adding header size constant to match HFPage header size
  private static final int HEADER_SIZE = 20;
  private static final int SLOT_SIZE = 4;
  private static final int MAX_ENTRIES = 125;

  // --------------------------------------------------------------------------

  /** Is this a temporary heap file, meaning it has no entry in the library? */
  protected boolean isTemp;

  /** The heap file name.  Null if a temp file, otherwise 
   * used for the file library entry. 
   */
  protected String fileName;

  /** First page of the directory for this heap file. */
  protected PageId headId;

  // --------------------------------------------------------------------------

  /**
   * If the given name is in the library, this opens the corresponding
   * heapfile; otherwise, this creates a new empty heapfile. 
   * A null name produces a temporary file which
   * requires no file library entry.
   */
  public HeapFile(String name) {

	  //set the file name and declare exists to check if in Library
	  fileName = name;
	  boolean exists = false;
	  
	  //if name is null, it is a temp file
	  if(fileName == null){
		  isTemp = true;
	  }
	  
	  //otherwise not temp file
	  //and need to check if already in Library
	  else{
		  isTemp = false;
		  
		  //if exists, need headID to be first page's PageID of file
		  headId = Minibase.DiskManager.get_file_entry(fileName);
		  if(headId != null){
			  exists = true;
		  }
	  }
	  
	  //if doesn't exist, need to make new Directory Page
	  //allocate new page (returns pinned PageID)
	  //add name/ID to library
	  if(!exists){
		  DirPage dpage = new DirPage();
		  headId = Minibase.BufferManager.newPage(dpage, 1);
		  dpage.setCurPage(headId);
		  Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
		  
		  if(!isTemp){
			  Minibase.DiskManager.add_file_entry(fileName, headId);
		  }
	  }
	  
	  
	  //throw new UnsupportedOperationException("Not implemented");

  } // public HeapFile(String name)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {

	  if(isTemp){
		  deleteFile();
	  }
	  
	    //throw new UnsupportedOperationException("Not implemented");

  } // protected void finalize() throws Throwable

  /**
   * Deletes the heap file from the database, freeing all of its pages
   * and its library entry if appropriate.
   */
  public void deleteFile() {

	  DirPage dpage = new DirPage();  ////////////is it possible to just use the headID rather than create new var?????????
	  PageId did = new PageId();
	  did.copyPageId(headId);
	  
	  //a temp page id to hold next id for while loop
	  PageId next = new PageId();
	  
	  int entryCnt;
	  
	  //cycle through the directory pages for the file to free all pages
	  while(did.pid != INVALID_PAGEID){
		  //pin the current directory pageId
//		  System.out.println("delete file pin page: " + did.pid);
		  Minibase.BufferManager.pinPage(did, dpage, PIN_DISKIO);
		  
		  //how many PageIDs to free on this directory page
		  entryCnt = dpage.getEntryCnt();
		  
		  //free each page on this dir page
		  for(int i = 0; i < entryCnt; i++){
			  Minibase.BufferManager.freePage(dpage.getPageId(i));
		  }
		  
		  //get the next directory page before freeing curr dir page
		  next.copyPageId(dpage.getNextPage());
//		  System.out.println("delete file unpin page: " + did.pid);
		  Minibase.BufferManager.unpinPage(did, UNPIN_CLEAN);
		  Minibase.BufferManager.freePage(did);
		  
		  //set the did to the next dir page id
		  did.copyPageId(next);
		  
		  //delete from library entry if it is not a temp file
		  if(!isTemp){
			  Minibase.DiskManager.delete_file_entry(fileName);
		  }
		  
	  }
	  
	 //throw new UnsupportedOperationException("Not implemented");

  } // public void deleteFile()

  /**
   * Inserts a new record into the file and returns its RID.
   * Should be efficient about finding space for the record.
   * However, fixed length records inserted into an empty file
   * should be inserted sequentially.
   * Should create a new directory and/or data page only if
   * necessary.
   * 
   * @throws IllegalArgumentException if the record is too 
   * large to fit on one data page
   */
  public RID insertRecord(byte[] record) {
	  
	  int len = record.length;

	  //error if to big of record
	  if(len > (PAGE_SIZE - HEADER_SIZE)){
		  throw new IllegalArgumentException("record length exceeds page size");
	  }
	  
	  PageId pageId = new PageId();
	  DataPage dataPage = new DataPage();
	  RID rid;
	  short freeSpace;
	  
	//find a page with enough free space or create a new page (if needed)
	  pageId.copyPageId(getAvailPage(len));
	  
	  //pin the pageno
//	  System.out.println("insert record pin page: " + pageId.pid);
	  Minibase.BufferManager.pinPage(pageId, dataPage, PIN_DISKIO);//diski0 needed???????????????
	  
	  //insert the record and return the rid
	  rid = dataPage.insertRecord(record);  //is the copy method needed here?????????????????????
	  
	  //need the free space left after insert to update the directory page
	  freeSpace = dataPage.getFreeSpace();
	  
	  //unpin athe datapage as dirty
//	  System.out.println("insert record unpin page: " + pageId.pid);
	  Minibase.BufferManager.unpinPage(pageId, UNPIN_DIRTY);
	  
	  //update the dir page
	  updateDirEntry(pageId, 1, freeSpace);
	  
	  return rid;
	  
	 //throw new UnsupportedOperationException("Not implemented");
   } // public RID insertRecord(byte[] record)

  /**
   * Reads a record from the file, given its rid.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) {
	  
	  DataPage dataPage = new DataPage();
	  PageId dataId = new PageId();
	  dataId.copyPageId(rid.pageno);
	  
	  byte record[];
	  
	  //pin the page referenced by the rid
//	  System.out.println("select record pin page: " + dataId.pid);
	  Minibase.BufferManager.pinPage(dataId, dataPage, PIN_DISKIO);
	  
	  //assign "record" to record on page based on slot referenced by rid
	  //unpin clean because no changes
	  //if error, throw exception
	  try{
		  record = dataPage.selectRecord(rid);
//		  System.out.println("select record unpin page: " + dataId.pid);
		  Minibase.BufferManager.unpinPage(dataId, UNPIN_CLEAN);
		  return record;
	  }
	  catch(IllegalArgumentException invalid){
//		  System.out.println("select record unpin page: " + dataId.pid);
		  Minibase.BufferManager.unpinPage(dataId, UNPIN_CLEAN);
		  throw new IllegalArgumentException("RID is invalid");
	  }

	    //throw new UnsupportedOperationException("Not implemented");

  } // public byte[] selectRecord(RID rid)

  /**
   * Updates the specified record in the heap file.
   * 
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord) {

	  DataPage dataPage = new DataPage();
	  PageId dataId = new PageId();
	  dataId.copyPageId(rid.pageno);
	  
	  //pin the page referenced by the rid
//	  System.out.println("update record pin page: " + dataId.pid);
	  Minibase.BufferManager.pinPage(dataId, dataPage, PIN_DISKIO);
	  
	  //try to update the record
	  //unpin dirty because change made to page
	  //if error, throw excption
	  try{
		  dataPage.updateRecord(rid, newRecord);
//		  System.out.println("update record unpin page: " + dataId.pid);
		  Minibase.BufferManager.unpinPage(dataId, UNPIN_DIRTY);
	  }
	  catch(IllegalArgumentException invalid){
//		  System.out.println("update record unpin page: " + dataId.pid);
		  Minibase.BufferManager.unpinPage(dataId, UNPIN_CLEAN);
		  throw new IllegalArgumentException("can't update record, invalid rid or new record");
	  }
	    //throw new UnsupportedOperationException("Not implemented");

  } // public void updateRecord(RID rid, byte[] newRecord)

  /**
   * Deletes the specified record from the heap file.
   * Removes empty data and/or directory pages.
   * 
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid) {
	  
	  DataPage dataPage = new DataPage();
	  PageId dataId = new PageId();
	  dataId.copyPageId(rid.pageno);
	  
	  short freeSpace;
	  
	  //pin the page referenced by the rid
//	  System.out.println("delete record pin page: " + dataId.pid);
	  Minibase.BufferManager.pinPage(dataId, dataPage, PIN_DISKIO);
	  
	  //try to delete the record, get the new freeSpace on the page
	  //need to unpin dirty since updated page and update the directory page
	  //if error, throw exception
	  try{
		  dataPage.deleteRecord(rid);
		  freeSpace = dataPage.getFreeSpace();
//		  System.out.println("delete record unpin page: " + dataId.pid);
		  Minibase.BufferManager.unpinPage(dataId, UNPIN_DIRTY);
		  updateDirEntry(dataId, -1, freeSpace);
	  }
	  catch(IllegalArgumentException invlaid){
//		  System.out.println("delete record unpin page: " + dataId.pid);
		  Minibase.BufferManager.unpinPage(dataId, UNPIN_CLEAN);
		  throw new IllegalArgumentException("can't delete, rid is invalid");
	  }

	    //throw new UnsupportedOperationException("Not implemented");

  } // public void deleteRecord(RID rid)

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {

	  DirPage directPage = new DirPage();
	  PageId directId = new PageId();
	  directId.copyPageId(headId);
	  
	  int entryCount, recordCount = 0;
	  
	  //need to keep a temporary "next" Directory Page ID
	  PageId next = new PageId();
	  
	  //on each directory page, loop through all the slots
	  //to accumulate the total record count for the file
	  while(directId.pid != INVALID_PAGEID){
		  //pin the current directory page
//		  System.out.println("get record count pin page: " + directId.pid);
		  Minibase.BufferManager.pinPage(directId, directPage, PIN_DISKIO);
		  //count of entries on this page
		  entryCount = directPage.getEntryCnt();
		  
		  //loop thru all entries on this page
		  for(int i = 0; i < entryCount; i++){
			  recordCount += directPage.getRecCnt(i);
		  }
		  
		  //get the next directory pageId for the while loop and unpin (clean)
		  next = directPage.getNextPage();
//		  System.out.println("get record count unpin page: " + directId.pid);
		  Minibase.BufferManager.unpinPage(directId, UNPIN_CLEAN);
		  directId.copyPageId(next);
	  }
	  
	  return recordCount;	  
	  
	    //throw new UnsupportedOperationException("Not implemented");

  } // public int getRecCnt()

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Searches the directory for the first data page with enough free space to store a
   * record of the given size. If no suitable page is found, this creates a new
   * data page.
   * A more efficient implementation would start with a directory page that is in the
   * buffer pool.
   */
  protected PageId getAvailPage(int reclen) {

	  DirPage directPage = new DirPage();
	  PageId directId = new PageId();
	  directId.copyPageId(headId);
	  
	  //need 2 more Page IDs to keep track of the temp "next" for while loop
	  //and the found "available" page
	  PageId avail, next = new PageId();
	  
	  //EntryCount will keep track of entries on the curr directory page
	  int entryCount = 0;
	  
	  //cycle thru each entry on each directory page until a page with
	  //enough room (including the slot size for record info) found
	  while(directId.pid != INVALID_PAGEID){
//		  System.out.println("get avail pin page: " + directId.pid);
		  Minibase.BufferManager.pinPage(directId, directPage, PIN_DISKIO);
		  entryCount = directPage.getEntryCnt();
		  
		  //loop thru entries of curr page looking for enough space
		  for(int i = 0; i < entryCount; i++){
			  if(directPage.getFreeCnt(i) >= (reclen + SLOT_SIZE)){
				  avail = directPage.getPageId(i);
//				  System.out.println("get avail unpin page: " + directId.pid);
				  Minibase.BufferManager.unpinPage(directId, UNPIN_CLEAN);
				  return avail;
			  }
		  }
		  
		  //if get here, haven't found good page, so go to next directory page
		  next.copyPageId(directPage.getNextPage());
//		  System.out.println("get avail unpin page: " + directId.pid);
		  Minibase.BufferManager.unpinPage(directId, UNPIN_CLEAN);
		  directId.copyPageId(next);
	  }
	  
	  //if get here, a good page doesn't exist, so create a new page
	  avail = insertPage();
	  return avail;
	  
	    //throw new UnsupportedOperationException("Not implemented");

  } // protected PageId getAvailPage(int reclen)

  /**
   * Helper method for finding directory entries of data pages.
   * A more efficient implementation would start with a directory
   * page that is in the buffer pool.
   * 
   * @param pageno identifies the page for which to find an entry
   * @param dirId output param to hold the directory page's id (pinned)
   * @param dirPage output param to hold directory page contents
   * @return index of the data page's entry on the directory page
   */
  protected int findDirEntry(PageId pageno, PageId dirId, DirPage dirPage) {

	  //note: dirId and dirPage already created and passed as parameters
	  //      pageno is the page we changed and need to update the directory
	  
	  //set dirId to the first page in directory
	  dirId.copyPageId(headId);
	  
	  //keep track of next directory page for loop
	  PageId next = new PageId();
	  
	  //keep track of the entry count on the current directory page
	  int entryCount;
	  
	  //loop through the entries on each directory page
	  //until the entry containing the pageno is found
	  while(dirId.pid != INVALID_PAGEID){
//		  System.out.println("find dir entry pin page: " + dirId.pid);
		  Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
		  entryCount = dirPage.getEntryCnt();
		  
		  //loop thru the entries of curr dirPage
		  for(int i = 0; i < entryCount; i++){
			  //check for slot that contains the pageno
			  if(dirPage.getPageId(i).equals(pageno)){
//				  System.out.println("find dir entry unpin page: " + dirId.pid);
				  Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
//				  System.out.print("dirID: " + dirId.pid + " , ");
				  return i;
			  }
		  }
		  
		  //if get here, haven't found the entry, so go to next dirPage
		  next.copyPageId(dirPage.getNextPage());
//		  System.out.println("find dir entry unpin page: " + dirId.pid);
		  Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
		  dirId.copyPageId(next);
	  }
	  
	  //if get here, something went terribly wrong because the dirPage entry should have
	  //been found!
	  throw new IllegalArgumentException("something went wrong, can't find the directory entry");
	  
	    //throw new UnsupportedOperationException("Not implemented");

  } // protected int findEntry(PageId pageno, PageId dirId, DirPage dirPage)

  /**
   * Updates the directory entry for the given data page.
   * If the data page becomes empty, remove it.
   * If this causes a dir page to become empty, remove it
   * @param pageno identifies the data page whose directory entry will be updated
   * @param deltaRec input change in number of records on that data page
   * @param freecnt input new value of freecnt for the directory entry
   */
  protected void updateDirEntry(PageId pageno, int deltaRec, int freecnt) {

	  DirPage directPage = new DirPage();
	  PageId directId = new PageId();
	  int slot, recordCount = 0;
	  
	  //findDirEntry will return the slot number and give the directPage and directId
	  slot = findDirEntry(pageno, directId, directPage);
	  
	  //pin the page
//	  System.out.println("dirID: " + directId.pid);
//	  System.out.println("update dir entry pin page: " + directId.pid);
	  Minibase.BufferManager.pinPage(directId, directPage, PIN_DISKIO);
	  	  
	  //new record count for the directory entry will be current count + "deltaRec" parameter
	  recordCount = directPage.getRecCnt(slot) + deltaRec;
	  
	  //if the record count for the page is 0, delete the page
	  //otherwise update the slot number's record count and free space
	  if(recordCount < 1){
		  deletePage(pageno, directId, directPage, slot); //unpinned in deletePage method
	  }
	  else{
		  directPage.setFreeCnt(slot, (short) freecnt);
		  directPage.setRecCnt(slot, (short) recordCount);
//		  System.out.println("update dir entry unpin page: " + directId.pid);
		  Minibase.BufferManager.unpinPage(directId, UNPIN_DIRTY);
	  }
	  
	    //throw new UnsupportedOperationException("Not implemented");

  } // protected void updateEntry(PageId pageno, int deltaRec, int deltaFree)

  /**
   * Inserts a new empty data page and its directory entry into the heap file. 
   * If necessary, this also inserts a new directory page.
   * Leaves all data and directory pages unpinned
   * 
   * @return id of the new data page
   */
  protected PageId insertPage() {
	  
	  DirPage directoryPage = new DirPage();
	  PageId directoryId = new PageId();
	  directoryId.copyPageId(headId);
	  
	  //keep track of next directory id for looping
	  PageId next = new PageId();
	  
	  //may need a new directory page
	  DirPage newDirectoryPage = new DirPage();
	  PageId newDirectoryId = new PageId();
	  
	  //will definitely need new data page since no room on prev data pages
	  DataPage newDataPage = new DataPage();
	  PageId newDataId = new PageId();
	  
	  //keep track of entry count for curr directory page
	  int entryCount = 0;
	  
	  //will need to set free space for the new page in the directory
	  int freeSpace = 0;
	  
	  //get the directory id for the page info of page inserting
	  //make new directory page/id if needed
	  while(true){
		  //pin current directory page
//		  System.out.println("insert page pin page: " + directoryId.pid);
		  Minibase.BufferManager.pinPage(directoryId, directoryPage, PIN_DISKIO);
		  
		  //if there is space on the current page, just use that page!
		  entryCount = directoryPage.getEntryCnt();
		  if(entryCount < MAX_ENTRIES){
//			  System.out.println("insert page unpin page: " + directoryId.pid);
			  Minibase.BufferManager.unpinPage(directoryId, UNPIN_CLEAN);
			  break;
		  }
		  
		  //if the next directory page is invalid, make a new directory page
		  next = directoryPage.getNextPage();
		  if(next.pid == INVALID_PAGEID){
			  //allocate a new page
			  newDirectoryId = Minibase.BufferManager.newPage(newDirectoryPage, 1);
//			  System.out.println("insert page (new dir page) pin page: " + newDirectoryId.pid);
			  
			  //set the page references in new directory page and old directory page
			  newDirectoryPage.setCurPage(newDirectoryId);
			  newDirectoryPage.setPrevPage(directoryId);
			  
			  directoryPage.setNextPage(newDirectoryId);
			  
			  //unpin the directory page dirty
//			  System.out.println("insert page unpin page: " + directoryId.pid);
			  Minibase.BufferManager.unpinPage(directoryId, UNPIN_DIRTY);
			  Minibase.BufferManager.unpinPage(newDirectoryId, UNPIN_CLEAN);
			  
			  //set directory page/id to the new directory page/id to use later
			  directoryId.copyPageId(newDirectoryId);
//			  directoryId = newDirectoryId;
			  directoryPage.setData(newDirectoryPage.getData());
			  
			  break;			  
		  }
		  
		  //if get here, need unpin and check the next valid directory page for an open slot
//		  System.out.println("insert page unpin page: " + directoryId.pid);
		  Minibase.BufferManager.unpinPage(directoryId, UNPIN_CLEAN);
		  directoryId.copyPageId(next);
	  }
	  
	  //now the directory page and ID is known
	  //so allocate space for new page and update the directory page for data page slot
	  
	  newDataId = Minibase.BufferManager.newPage(newDataPage, 1);
//	  System.out.println("insert page (new data page) pin page: " + newDataId.pid);
//	  System.out.println("insert page (dir page) pin page: " + directoryId.pid);
	  Minibase.BufferManager.pinPage(directoryId, directoryPage, PIN_DISKIO);
	  
	  //make sure count and free space is correct for slot number
	  entryCount = directoryPage.getEntryCnt();
	  freeSpace = newDataPage.getFreeSpace();
	  
	  directoryPage.setPageId(entryCount, newDataId);
	  directoryPage.setRecCnt(entryCount, (short) 0);
	  directoryPage.setFreeCnt(entryCount, (short) freeSpace);
	  
	  entryCount++;
	  directoryPage.setEntryCnt((short) entryCount);
	  
	  newDataPage.setCurPage(newDataId);
	  
	  //unpin directory page and new data page
//	  System.out.println("insert page (dir page) unpin page: " + directoryId.pid);
//	  System.out.println("insert page (new data page) unpin page: " + newDataId.pid);
	  Minibase.BufferManager.unpinPage(directoryId, UNPIN_DIRTY);
	  Minibase.BufferManager.unpinPage(newDataId, UNPIN_CLEAN);
	  
	  return newDataId;

	  //  throw new UnsupportedOperationException("Not implemented");

  } // protected PageId insertPage()

  /**
   * Deletes the given data page and its directory entry from the heap file. If
   * appropriate, this also deletes the directory page.
   * 
   * @param pageno identifies the page to be deleted
   * @param dirId input param id of the directory page holding the data page's entry
   * @param dirPage input param to hold directory page contents
   * @param index input the data page's entry on the directory page
   */
  protected void deletePage(PageId pageno, PageId dirId, DirPage dirPage,
      int index) {
	  //note: page is pinned from prior to this method call
	  
	  //if deleting a directory page, will need to reassign next and prev directory pages
	  DirPage tempPage = new DirPage();
	  PageId prev = new PageId();
	  prev.copyPageId(dirPage.getPrevPage());
	  PageId next = new PageId();
	  next.copyPageId(dirPage.getNextPage());
	  	  
	  //keep track of the curr dir page entry count
	  int entryCount = 0;
	  
	  //first free the page
	  Minibase.BufferManager.freePage(pageno);
	  
	  //if there are more pages in the directory page
	  //just compact the info and decrement the entry count
	  //also do this if the curr page is the head id because don't want to remove that one
	  if(entryCount > 1 || dirId.equals(headId)){
		  dirPage.compact(index);
		  entryCount--;
		  dirPage.setEntryCnt((short) entryCount);
		  Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
	  }

	  //otherwise, the directory page is empty and need to 
	  //reset prev and next references to to eachother instead of to curr page
	  else{
		  if(prev.pid != INVALID_PAGEID){
			  Minibase.BufferManager.pinPage(prev, tempPage, PIN_DISKIO);
			  tempPage.setNextPage(next);
			  Minibase.BufferManager.unpinPage(prev, UNPIN_DIRTY);
		  }
		  if(next.pid != INVALID_PAGEID){
			  Minibase.BufferManager.pinPage(next, tempPage, PIN_DISKIO);
			  tempPage.setNextPage(prev);
			  Minibase.BufferManager.unpinPage(next, UNPIN_DIRTY);			  
		  }
		  
		  //unpin the current directory page and then free it
		  Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
		  Minibase.BufferManager.freePage(dirId);
	  }
	  
	    //throw new UnsupportedOperationException("Not implemented");

  } // protected void deletePage(PageId, PageId, DirPage, int)

} // public class HeapFile implements GlobalConst
