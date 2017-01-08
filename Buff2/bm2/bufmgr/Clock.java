package bufmgr;

public class Clock {

	int curr;
	int frames;
	
	
	Clock(int num){
		frames = num;
		curr = 0;
	}
	
	public int pickVictim(FrameDesc[] pool){
		
		//go thru frames to find victim
		for(int i = 0; i < 2 * frames; i++){
			//reset curr when end of frames
			if(curr == frames)
				curr = 0;
			
			//if data not valid, this is the victim
			if(!pool[curr].isValid()){
				return curr;
			}
			
			//check for possible victims with pin count 0
			if(pool[curr].isZeroCount()){
				
				//if refbit true, give second chance
				if(pool[curr].isRef()){
					pool[curr].noRef();
				}
				
				//otherwise we found the victim
				else{
					return curr;
				}
			}
			
			curr++;
		}
		
		throw new IllegalStateException("no victims availible");
		

	}
}
