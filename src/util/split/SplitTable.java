package util.split;

public class SplitTable {
	static int MB = 1024*1024;
	public static int minimumSize[] = { 1, 1, 1, 1, 1, 1, 1, 1,
						  72*MB, 80*MB, 88*MB,
						  96*MB, 104*MB, 112*MB,
						  120*MB, 128*MB, 136*MB,
						  144*MB, 152*MB, 160*MB,
						  168*MB, 176*MB, 184*MB,
						  192*MB, 200*MB, 208*MB,
						  216*MB, 224*MB, 232*MB,
						  240*MB, 248*MB, 256*MB,
						  264*MB, 272*MB, 280*MB,
						  288*MB, 296*MB, 304*MB,
						  312*MB, 320*MB 
	};
	
	public static long maximumSize[] = { 8*MB, 16*MB, 24*MB, 32*MB, 40*MB, 48*MB, 56*MB, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
						   Long.MAX_VALUE, Long.MAX_VALUE 
						   
	};
	
	public static long getMapred_min_split_size(int splitSizeMB) {
		//return Math.max(minSize, Math.min(maxSize, blockSize));
		/*
		long minSplit = 1L;
		long maxSplit = Long.MAX_VALUE;
		if(splitSizeMB < blockSizeMB) 
			maxSplit = splitSizeMB * 1024 * 1024;
		else
			minSplit = splitSizeMB * 1024 * 1024;
		
		return minSplit;
		*/
		return splitSizeMB * 1024 * 1024;
	}
	
	public static long getMapred_max_split_size(int splitSizeMB) {
		/*
		long minSplit = 1L;
		long maxSplit = Long.MAX_VALUE;
		if(splitSizeMB < blockSizeMB) 
			maxSplit = splitSizeMB * 1024 * 1024;
		else
			minSplit = splitSizeMB * 1024 * 1024;
		
		return maxSplit;
		*/
		return splitSizeMB * 1024 * 1024;
	}
	
}
