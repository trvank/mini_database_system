package bufmgr;

import global.PageId;

public class FrameDesc {
	
	boolean dirty;
	boolean valid;
	boolean refbit;
	PageId pageNum;
	int pinCount;
	
	//may want to include replacement algorithm info here
	
	FrameDesc(){
		valid = false;
		pageNum = new PageId();
	}
	
	FrameDesc(PageId num){
		pageNum = num;
		pinCount = 1; //when initialized, count is 1
		dirty = false;
		refbit = true; //refbit set
		valid = true;
		
		
	}
	
	public void pinUp(){
		pinCount++;
	}
	
	public void pinDown(){
		pinCount--;
	}
	
	public void makeDirty(){
		dirty = true;
	}
	
	public void makeClean(){
		dirty = false;
	}
	
	public void makeInvalid(){
		valid = false;
	}
	
	public void ref(){
		refbit = true;
	}
	
	public void noRef(){
		refbit = false;
	}
	
	public boolean isZeroCount(){
		if (pinCount == 0){
			return true;
		}
		
		else{
			return false;
		}
	}
	
	public boolean isValid(){
		if(valid){
			return true;
		}
		
		else{
			return false;
		}
	}
	
	public boolean isRef(){
		if(refbit){
			return true;
		}
		
		else{
			return false;
		}
	}
	
	public boolean isDirty(){
		if(dirty){
			return true;
		}
		
		else{
			return false;
		}
	}
	
	public void setPageId(int newNum){
		pageNum.pid = newNum;
	}
	
	public PageId getPageNum(){
		return pageNum;
	}
	
	public int getPinCount(){
		return pinCount;
	}

}
