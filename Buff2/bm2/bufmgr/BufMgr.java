package bufmgr;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {

	public Page[] frames;
	FrameDesc[] frametab;
	HashMap<Integer, Integer> map; //map page id to slot number
	Clock replPolicy;
	int numPages; //index into buffer pool before full
	int numFrames;

  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
  public BufMgr(int numframes) {
	  
	//initialize ints
	  numPages = 0;
	  numFrames = numframes;
	  
	//initialize buffers
	  frames = new Page[numframes];
	  frametab = new FrameDesc[numframes];
	  
	  for (int i = 0; i < numframes; i++){
		  frames[i] = new Page();
		  frametab[i] = new FrameDesc();
	  }
	  
	  //initialize map
	  map = new HashMap<Integer, Integer>();
	  
	  //initialize replacement policy (clock)
	  replPolicy = new Clock(numFrames);

    //throw new UnsupportedOperationException("Not implemented");

  } // public BufMgr(int numframes)

  /**
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments 
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {

	int index;
	
	//if the page id is already in the map, update the pin count
	//and make sure referenced
	//need to make sure the Page parameter shares the data of frame in buffer pool
	if(map.containsKey(pageno.hashCode())){
		index = map.get(pageno.hashCode());
		mempage.setPage(frames[index]);
		frametab[index].pinUp();
		frametab[index].ref();
	}
	
	//otherwise the page id is not in the map
	//so add to the buffer pool (and map)
	else{		
		//if free space, use that first
		  if(numPages < numFrames){
			  index = numPages;
			  numPages++;
		  }

		  //otherwise, pick a replacement page
		  else{

			  //get replacement index into buffer pool from the replacement policy
			  index = replPolicy.pickVictim(frametab);

			  //write victim to disk
			  if(frametab[index].isValid() && frametab[index].isDirty()){
				  Minibase.DiskManager.write_page(frametab[index].getPageNum(), frames[index]);
			  }

			  //remove the victim from the map
			  map.remove(frametab[index].getPageNum().hashCode());

		  }
		  
		  //add the page id & index value to the map
		  //initialize a new PageID and add it to the frame description array
		  map.put(pageno.hashCode(), index);
		  PageId newPageNum = new PageId(pageno.hashCode());
		  frametab[index] = new FrameDesc(newPageNum);
		  
		  //for Disk I/O, read contents into the buffer pool 
		  if(contents == PIN_DISKIO){
			  Minibase.DiskManager.read_page(pageno, frames[index]);
		  }
		  
		  //for Memcpy, copy the mempage into the buffer pool
		  //NOTE: the application still passes tests w/o this else if,
		  //but including because in the method description
		  else if(contents == PIN_MEMCPY){
			  frames[index].copyPage(mempage);
		  }
		  
		  //need to make sure the Page parameter shares the data of frame in buffer pool 
		  mempage.setPage(frames[index]);
		  
	}
	
	
	  
	  //throw new UnsupportedOperationException("Not implemented");

  } // public void pinPage(PageId pageno, Page page, int contents)
  
  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {

	  //error if pageID not in map
	  if(!map.containsKey(pageno.hashCode())){
		  throw new IllegalArgumentException("page not in buffer pool");
	  }
	  
	  //find index into the buffer pool
	  int index = map.get(pageno.hashCode());
	  
	  //error if the page is already unpinned
	  if(frametab[index].isZeroCount()){
		  throw new IllegalArgumentException("page is not pinned");
	  }

	  //if the page is pinned
	  if(!frametab[index].isZeroCount()){
		  
		  //decrement pin count
		  frametab[index].pinDown();
		}  
		  //if dirty, make sure dirty marked in frame table
		  if(dirty){
			  frametab[index].makeDirty();
		  }
	  
    //throw new UnsupportedOperationException("Not implemented");

  } // public void unpinPage(PageId pageno, boolean dirty)
  
  /**
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page firstpg, int run_size) {

	  //allocate the disk pages and get the first pageID available from disk
	  PageId firstPgId = Minibase.DiskManager.allocate_page(run_size);
	  
	  //error if the first pageID is already pinned
	  if(map.containsKey(firstPgId.hashCode())){
		  throw new IllegalArgumentException("already pinned firstPgId: " + firstPgId.hashCode());
	  }
	  
	  //error if buffer pool is full
	  if(getNumUnpinned() <= 0){
		  throw new IllegalArgumentException("buffer pool is full");
	  }
	  
	  //pin the first PageID and return the PageID to caller
	  pinPage(firstPgId, firstpg, PIN_MEMCPY);  
	  
	  return firstPgId;
	  
    //throw new UnsupportedOperationException("Not implemented");

  } // public PageId newPage(Page firstpg, int run_size)

  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {

	//if the pageno is not in the map, just deallocate
	  if(!map.containsKey(pageno.hashCode())){
		  Minibase.DiskManager.deallocate_page(pageno);
	  }

	  //Otherwise, need to free from buffer pool first
	  else{

		  //get index of pageID inot buffer pool
		  int idx = map.get(pageno.hashCode());

		  //error if page is pinned, can't free page in use
		  if(!frametab[idx].isZeroCount()){
			  throw new IllegalArgumentException("Page is pinned");
		  }

		  //page is no longer valid so immediately up for replacement
		  //make sure clean since no need to write to disk even if it was dirty
		  //remove pageID from the map and deallocate the page
		  frametab[idx].makeInvalid();
		  frametab[idx].makeClean();
		  map.remove(pageno.hashCode());
		  Minibase.DiskManager.deallocate_page(pageno);

	  }
	  
    //throw new UnsupportedOperationException("Not implemented");

  } // public void freePage(PageId firstid)

  /**
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   * 
   */
  public void flushAllFrames() {

	//walk thru all frames to write to disk (if dirty & valid) and make clean
	  //don't make invalid since want to keep the ref bit check for replacement
	  for(int i = 0; i < numFrames; i++){
		  if(frametab[i].isDirty() && frametab[i].isValid()){
			  Minibase.DiskManager.write_page(frametab[i].getPageNum(), frames[i]);
			  frametab[i].makeClean();
		  }
	  }
	  
    //throw new UnsupportedOperationException("Not implemented");

  } // public void flushAllFrames()

  /**
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageno) {
	  
	  //get the index of the pageID into the buffer pool
	  int idx = map.get(pageno.hashCode());
	  
	  //error if the page is pinned
	  if(!frametab[idx].isZeroCount()){
		  throw new IllegalArgumentException("cannot flush pinned page :" + pageno.hashCode());
	  }
	  
	  //write page to disk (if dirty and valid) and make it clean
	  if(frametab[idx].isDirty() && frametab[idx].isValid()){
		  Minibase.DiskManager.write_page(pageno, frames[idx]);
		  frametab[idx].makeClean();
	  }
	  
	//throw new UnsupportedOperationException("Not implemented");
    
  }

   /**
   * Gets the total number of buffer frames.
   */
  public int getNumFrames() {
    
	  return numFrames;
	  
	  //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {
    
	  int num = 0;
	  
	  //walk thru frames and count the number of '0' pin counts
	  //stupid way for now, look at keeping track as pinned/unpinned
	  for(int i = 0; i < numFrames; i++){
		  if(frametab[i].getPinCount() == 0){
			  num++;
		  }
	  }
	  
	  return num;
	  
	  //throw new UnsupportedOperationException("Not implemented");
  }

} // public class BufMgr implements GlobalConst
