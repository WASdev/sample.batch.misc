package com.ibm.websphere.samples.batch.artifacts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.batch.api.BatchProperty;
import javax.batch.api.chunk.listener.ChunkListener;
import javax.batch.api.chunk.listener.ItemProcessListener;
import javax.batch.api.chunk.listener.ItemReadListener;
import javax.batch.api.chunk.listener.ItemWriteListener;
import javax.batch.api.listener.StepListener;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import com.ibm.jzos.ZUtil;

@Dependent
/**
 * This class implements several JSR-352 batch listener interfaces to collect performance statistics about a chunk step.
 * The listeners get control around the Reader, Processor, Writer, and around a whole chunk.
 * Elapsed times for each element are recorded.  On some platforms CPU time may also be recorded (z/OS at least).
 * Data is written at the end of each chunk or at the end of the whole step (configurable).
 * Be careful waiting until the end of the step as steps process a large number of records may accumulate a vast 
 * amount of data along the way.
 * Data can be written to files in comma-separated-value (CSV) format or just logged to the job output.
 * Configure the listener by including this block of XML in the JSL for the chunk step:
 * 
 * 		<listeners>
 *			<listener ref="ccom.ibm.websphere.samples.batch.artifacts.ChunkTimeListener">
 *				<properties >
 *				<property name="outputDir" value="#{jobParameters['outputDir']}"/>
 *				<property name="writeAt" value="#{jobParameters['writeAt']}?:ChunkEnd;"/>
 *				<property name="logToJoblog" value="#{jobParameters['logToJoblog']}?:false;"/>
 *				</properties>
 *			</listener>
 *		</listeners>
 *
 * Other listener implementations can also be included as the spec allows multiple instances of each listener.
 * Parameters are:
 * 
 *  outputDir - absolute path to where the .csv files should be written.  If not specified, no .csv data is written.  
 *              The files will be named as:
 *  				<job name>.<job execution id>.<step name>.ChunkData.csv
 *  				<job name>.<job execution id>.<step name>.IterationData.csv
 * 
 *  writeAt - set to ChunkEnd or StepEnd to indicate when to write the data.  Any other value will cause no data to be written
 *  
 *  logToJoblog - indicates whether data should be logged to the job log (at step or chunk end as above).
 *                Independent of whether .csv data is being written so you can have both
 * 
 * @author follis
 *
 */
public class ChunkTimeListener implements ChunkListener, ItemReadListener, ItemProcessListener, ItemWriteListener, StepListener {
	
	/**
	 * Create the logger so we an write to the joblog
	 */
	private static final Logger log = Logger.getLogger( ChunkTimeListener.class.getName() );
	
	/**
	 * Define the lists of data collected for each read/process iteration and for each chunk.
	 */
	private LinkedList<ChunkData> chunkDataList = new LinkedList<ChunkData>();
	private LinkedList<IterationData> iterationDataList = new LinkedList<IterationData>();

	/**
	 * Set up the object to handle access to CPU data on platforms that support it
	 */
	private CPUData cpuData = new CPUData();
	
	/**
	 * Declare objects to contain data for the current chunk/iteration and keep the
	 * chunk and iteration counters (iteration counter resets for each chunk)
	 */
	private ChunkData currentChunkData;
	private IterationData currentIterationData;
	int currentChunkNumber;
	int currentIterationNumber;
	
	/**
	 * Variables to hold the buffered writers if we are writing .csv data to files
	 */
	BufferedWriter chunkWriter=null;
	BufferedWriter iterationWriter=null;

	/**
	 * Access the step context so we can lift the step name to create the .csv file name
	 */
	@Inject
	private StepContext stepContext;

	/**
	 * Access the job context so we can lift the job name and job execution id for the .csv file name
	 */
	@Inject
	private JobContext jobContext;

	/**
	 * Inject the output directory path from the JSL
	 */
	@Inject
	@BatchProperty(name="outputDir")
	String outputDir;
	
	/**
	 * Inject "StepEnd" or "ChunkEnd" to control when to write data
	 */
	@Inject
	@BatchProperty(name="writeAt")
	String writeAt;
	
	/**
	 * Inject the boolean (as a string "true" or "false") that controls whether to log to the joblog.
	 * Also declare the boolean we'll set from the injected string.
	 */
	@Inject
	@BatchProperty(name="logToJoblog")
	String logToJoblogS;
	boolean logToJoblog = false;
	    	
    /**
     * Default constructor. Nothing to do here
     */
    public ChunkTimeListener() {
    }

    /**
     * Here we get control before the step we're monitoring starts so we'll set everything up.
     * First we convert the injected true/false string into a real boolean that we'll use to control logging to the joblog.
     * Next, if we're writing .csv data to files, open up the chunk and iteration data files and write the
     * appropriate header (depending on whether or not we're able to get CPU data).  
     */
	@Override
	public void beforeStep() throws Exception {
		
		logToJoblog = (logToJoblogS.equalsIgnoreCase("true"));
		
		if (outputDir!=null) {
			String prefix = outputDir + "/" + jobContext.getJobName()+"."+jobContext.getExecutionId()+"."+stepContext.getStepName();
			String chunkWriteFilename = prefix + ".ChunkData.csv";
			String iterationWriteFilename = prefix + ".IterationData.csv";
			
		    chunkWriter = new BufferedWriter(new FileWriter(chunkWriteFilename));
			if (!cpuData.isAvailable) {
				chunkWriter.write(ChunkData.header);
				chunkWriter.newLine();
			} else {
				chunkWriter.write(ChunkData.headerWithCPU);
				chunkWriter.newLine();
			}
		    
		    iterationWriter = new BufferedWriter(new FileWriter(iterationWriteFilename));
			if (!cpuData.isAvailable) {
			    iterationWriter.write(IterationData.header);
			    iterationWriter.newLine();
			} else {
				iterationWriter.write(IterationData.headerWithCPU);
			    iterationWriter.newLine();			
			}
		}
		
	}

	/**
	 * The step is over, so we're all done.  Time to tidy up and get out of the way.
	 * If we're writing data at the end of the step, go do that now.
	 * If we're writing to .csv files, we're done with them so go close them.
	 */
	@Override
	public void afterStep() throws Exception {

    	if (writeAt.equalsIgnoreCase("StepEnd")) {
    		
    		if (outputDir!=null) {
    			writeData();
    		}
    		
    		if (logToJoblog) {
    			logData();
    		}
    	}	
    	
		if (outputDir!=null) {
			chunkWriter.close();
			iterationWriter.close();			
		}

	}

    
	/**
     * At the beginning of each chunk we need to create a new ChunkData object and increment the current
     * chunk number along the way.  Add the new object to the list and record data for the start of the chunk.
     */
    public void beforeChunk() {
    	
    	currentChunkData = new ChunkData(++currentChunkNumber);
    	chunkDataList.add(currentChunkData);
    	currentChunkData.startChunk();
    }

	/**
     * If something bad happens in the chunk...well..that's a shame..nothing to really do though.
     */
    public void onError(Exception arg0) {
    }

	/**
     * When the chunk ends we need to record data for when that happened and then tidy up for the next chunk
     * by resetting the iteration number.  
     * If we're writing data at the end of the chunk, go do that, then clear out the chunk and iteration data
     * lists (the chunk one will just have one entry) so we're ready for the next chunk.
     */
    public void afterChunk() throws Exception {
    	
    	currentChunkData.endChunk();  	
    	currentIterationNumber=0;
    	
    	if (writeAt.equalsIgnoreCase("ChunkEnd")) {
    		
    		if (outputDir!=null) {
    			writeData();
    		}
    		
    		if (logToJoblog) {
    			logData();
    		}
    		
    		// Wipe out data
    		chunkDataList.clear();
    		iterationDataList.clear();    		
    	}
    	
    }

    /**
     * We're starting a read, so that's a new Iteration.
     * Create an IterationData object to hold information about it and kick the iteration count up.
     * Add the object to the list of Iteration Data and record the data for the start of the read.
     */
	@Override
	public void beforeRead() throws Exception {
		currentIterationData = new IterationData(currentChunkNumber,++currentIterationNumber);
		iterationDataList.add(currentIterationData);
		currentIterationData.startRead();		
	}
	
	/**
	 * Something bad happened, so afterRead won't get control.  Note that we had an error.
	 * beforeRead will get control next so this Iteration Data won't have end-read data.  Might make the
	 * data look odd, so the read error count in the chunk data will help explain that.
	 * We might be able to do something nicer here..
	 */
	@Override
	public void onReadError(Exception arg0) throws Exception {
		currentChunkData.readError();
		
	}

	/**
	 * The read is over, so update tracking data appropriately
	 */
	@Override
	public void afterRead(Object arg0) throws Exception {
		currentIterationData.endRead();
		currentChunkData.readRecord();
	}

	/**
	 * About to start a process, so just record the data in the iteration data object.
	 */
	@Override
	public void beforeProcess(Object arg0) throws Exception {
		currentIterationData.startProcess();
		
	}

	/**
	 * Similar to a read error, we increment a count in the chunk to help explain the oddness in the iteration data.
	 * We might be able to handle this better, but hopefully you don't have a lot of errors when you're trying to
	 * gather performance data.
	 */
	@Override
	public void onProcessError(Object arg0, Exception arg1) throws Exception {
		currentChunkData.processError();
		
	}

	/**
	 * Processing is done, so record data appropriately.
	 */
	@Override
	public void afterProcess(Object arg0, Object arg1) throws Exception {
		currentIterationData.endProcess();
		currentChunkData.processedRecord();
	}

	/**
	 * We're done with this chunk so we'll start write processing.
	 * Record data for the start in the chunk (remember one write per chunk, not per iteration)
	 */
	@Override
	public void beforeWrite(List<Object> arg0) throws Exception {
		currentChunkData.startWrite();
	}

	/**
	 * Don't do anything.  Hoping this doesn't happen during performance runs..  
	 */
	@Override
	public void onWriteError(List<Object> arg0, Exception arg1) throws Exception {
	}

	/**
	 * Write is over, so record data about it
	 */
	@Override
	public void afterWrite(List<Object> arg0) throws Exception {
		currentChunkData.endWrite();
	}

	/**
	 * Time to write CSV data to the files.  We might be at the end of a chunk or the end of a step here.
	 * We don't care.  Just iterate over the linked lists and write all the data we have to the appropriate file.
	 * @throws Exception
	 */
	public void writeData() throws Exception {
		Iterator<ChunkData> it1;
	    it1 = chunkDataList.iterator();
	    while (it1.hasNext()){
	    	ChunkData cd = (ChunkData)it1.next();
	    	String data = cd.toCSV();
    		chunkWriter.write(data);
    		chunkWriter.newLine();
	    }

	    Iterator<IterationData> it2;
	    it2 = iterationDataList.iterator();
	    while (it2.hasNext()){
	    	IterationData id = (IterationData)it2.next();
	    	String data = id.toCSV();
    		iterationWriter.write(data);
    		iterationWriter.newLine();
	    }
	}
	
	/**
	 * Similar to writeData, we're at the end of a chunk or the step and we need to log the data we have.
	 * This is doing the same thing as writeData except it is logging it to the joblog.  
	 * We might be doing both things, depending on input parameters.  
	 */
	public void logData() {
		Iterator<ChunkData> it1;
	    it1 = chunkDataList.iterator();
	    while (it1.hasNext()){
	    	ChunkData cd = (ChunkData)it1.next();
	    	String data = cd.toString();
	    	log.log(Level.INFO, data);
	    }
	    
	    Iterator<IterationData> it2;
	    it2 = iterationDataList.iterator();
	    while (it2.hasNext()){
	    	IterationData id = (IterationData)it2.next();
	    	String data = id.toString();
	    	log.log(Level.INFO, data);
	    }	

	}
	
	/**
	 * An internal class to track data about a chunk iteration (a read/process cycle).
	 *
	 */
	public class IterationData {
		/** record when the read started */
		private Instant readStart;
		/** record when the read ended */
		private Instant readEnd;
		/** record CPU used by the thread at read start */
		private long readCPUStart;
		/** record CPU used by the thread at read end */
		private long readCPUEnd;
		/** record when the processing started */
		private Instant processStart;
		/** record when the processing ended */
		private Instant processEnd;
		/** record CPU used by the thread at processing start */
		private long processCPUStart;
		/** record CPU used by the thread at processing end */
		private long processCPUEnd;
		/** record the iteration number for this iteration (resets within each chunk) */
		private int iterationNumber;
		/** record the chunk this iteration is part of */
		private int chunkNumber;
		
		/**
		 * Constructor
		 * @param chunknum - the chunk this iteration is part of
		 * @param iternum - the number for this iteration within the chunk
		 */
		public IterationData(int chunknum, int iternum) {
			iterationNumber = iternum;
			chunkNumber = chunknum;
		}
		
		/**
		 * A read started, grab the time and CPU time on the thread
		 */
		public void startRead() { 
			readStart = Instant.now();
			readCPUStart = cpuData.getCPUTime();
		}
		
		/**
		 * A read ended, grab the time and CPU time on the thread
		 */
		public void endRead() { 
			readEnd = Instant.now();
			readCPUEnd = cpuData.getCPUTime();
		}
		
		/**
		 * A process started, grab the time and CPU time on the thread
		 */
		public void startProcess() {
			processStart = Instant.now();
			processCPUStart = cpuData.getCPUTime();
		}
		
		/**
		 * A process ended, grab the time and CPU time on the thread
		 */
		public void endProcess() {
			processEnd = Instant.now();
			processCPUEnd = cpuData.getCPUTime();
		}
		
		/**
		 * For logging purposes, convert the data into a readable string.
		 * Calculate durations for things we have start/end times for.
		 * If we're getting CPU data, print it.  If not, then don't.  
		 * @return The data as a readable string
		 */
		public String toString() {
			
			/* The next three lines are just to avoid might-not-be-initialized warnings */
			Instant temp = Instant.now(); 
			Duration read = Duration.between(temp, temp);
			Duration process = Duration.between(temp, temp); 
			
			if ((readStart!=null)&&(readEnd!=null))
   			    read = Duration.between(readStart, readEnd);
			if ((processStart!=null)&&(processEnd!=null))
			    process = Duration.between(processStart, processEnd);
			
			String retVal;
			if (!cpuData.isAvailable) {
				retVal = new String("Chunk#="+chunkNumber+" Iteration#="+iterationNumber+
						" Read Time = "+read.toMillis()+" Process Time = "+process.toMillis()); 
			} else {
				long readCPU = readCPUEnd - readCPUStart;
				long processCPU = processCPUEnd - processCPUStart;
				retVal = new String("Chunk#="+chunkNumber+" Iteration#="+iterationNumber+
						" Read Time = "+read.toMillis()+" Read CPU (micros) = "+readCPU + 
						" Process Time = "+process.toMillis()) + " Process CPU (micros) = "+ processCPU;
			}
			
			return retVal;
			
		}
		
		/** Headers for the CSV file */
	    public static final String header = "Chunk,Iter,ReadTMillis,ProcessTMillis";
	    public static final String headerWithCPU = "Chunk,Iter,ReadTMillis,ReadCPUmicro,ProcessTMillis,ProcessCPUmicro";
		
	    /**
	     * If we're writing to a .csv file we'll need the data separated by commas.
	     * @return The data as a comma-separated-value string
	     */
		public String toCSV() {
			
			/* Again, these next three lines are to avoid used-before-set warnings */
			Instant temp = Instant.now();
			Duration read = Duration.between(temp, temp);
			Duration process = Duration.between(temp, temp);
			
			if ((readStart!=null)&&(readEnd!=null))
   			    read = Duration.between(readStart, readEnd);
			if ((processStart!=null)&&(processEnd!=null))
			    process = Duration.between(processStart, processEnd);
			
			String retVal;
			if (!cpuData.isAvailable) {
				retVal = new String(chunkNumber+","+
		                  iterationNumber+","+
				          read.toMillis()+","+
		                  process.toMillis()); 
			} else {
				long readCPU = readCPUEnd - readCPUStart;
				long processCPU = processCPUEnd - processCPUStart;
				retVal = new String(chunkNumber+","+
		                  iterationNumber+","+
				          read.toMillis()+","+
		                  readCPU+"," +
		                  process.toMillis()) + "," +
						  processCPU;
			}
			
			return retVal;
		}
		
	}

	/**
	 * An internal class to track data about each chunk
	 */
	public class ChunkData {
		/** record when the chunk started */
		private Instant chunkStart;
		/** record when the chunk ended */
		private Instant chunkEnd;
		/** record CPU on the thread when the chunk started */
		private long chunkCPUStart;
		/** record CPU on the thread when the chunk ended */
		private long chunkCPUEnd;
		/** record when the write started */
		private Instant writeStart;
		/** record when the write ended */
		private Instant writeEnd;
		/** record CPU on the thread when the write started */
		private long writeCPUStart;
		/** record CPU on the thread when the write ended */
		private long writeCPUEnd;
		/** hold the number of this chunk */
		private int chunkNumber;
		/** how many reads were in this chunk? */
		private int readCount;
		/** how many processes were in this chunk? */
		private int processCount;
		/** how many reads threw exceptions? */
		private int readErrorCount;
		/** how many processes threw exceptions? */
		private int processErrorCount;
		
		/**
		 * Constructor - remember the chunk number
		 * @param curChunk - the chunk number
		 */
		public ChunkData(int curChunk) {chunkNumber = curChunk;}
		
		/**
		 * A chunk started, remember the starting data
		 */
		public void startChunk() {
			chunkStart = Instant.now();
			chunkCPUStart = cpuData.getCPUTime();
		}
		
		/**
		 * A chunk ended, remember the ending data
		 */
		public void endChunk() {
			chunkEnd = Instant.now();
			chunkCPUEnd = cpuData.getCPUTime();
		}
		
		/**
		 * A write is starting, remember the starting data
		 */
		public void startWrite() {
			writeStart = Instant.now();
			writeCPUStart = cpuData.getCPUTime();
		}
		
		/**
		 * A write ended, remember the data
		 */
		public void endWrite() {
			writeEnd = Instant.now();
			writeCPUEnd = cpuData.getCPUTime();
		}
		
		/** Bump the read count */
		public void readRecord() {++readCount;}
		/** Bump the process count */
		public void processedRecord() {++processCount;}
		/** Bump the read error count */
		public void readError() {++readErrorCount;}
		/** Bump the process error count */
		public void processError() {++processErrorCount;}
		
		/**
		 * We're logging data to the joblog, so create a readable string with the chunk data.
		 * @return the data as a string
		 */
		public String toString() {
			
			/* The next three lines are to avoid used-before-set warnings */ 
			Instant temp = Instant.now();
			Duration chunk = Duration.between(temp, temp);
			Duration write = Duration.between(temp, temp);
			
			if ((chunkStart!=null)&&(chunkEnd!=null))
			    chunk = Duration.between(chunkStart, chunkEnd);
			if ((writeStart!=null)&&(writeEnd!=null))
			    write = Duration.between(writeStart, writeEnd);
			
			String retVal;
			
			if (!cpuData.isAvailable) {
				retVal = new String("Chunk#="+chunkNumber+" Write Time = "+write.toMillis()+
						" Chunk Time = "+chunk.toMillis()+" Read Count = "+readCount + " readErrorCount = " + readErrorCount +
						" processCount = "+processCount+" processErrorCount = "+processErrorCount);
			} else {
				long writeCPU = writeCPUEnd - writeCPUStart;
				long chunkCPU = chunkCPUEnd - chunkCPUStart;
				retVal = new String("Chunk#="+chunkNumber+" Write Time = "+write.toMillis()+" Write CPU(micros) = "+writeCPU +
						" Chunk Time = "+chunk.toMillis()+" Chunk CPU(micros) = "+chunkCPU+
						" Read Count = "+readCount + " readErrorCount = " + readErrorCount +
						" processCount = "+processCount+" processErrorCount = "+processErrorCount);
			}

			return retVal; 
		}
		
		/** Header strings for the .csv file */
		public static final String header = "Chunk,WriteTMillis,ChunkTMillis,Reads,ReadErr,Processes,ProcessErr";
		public static final String headerWithCPU = "Chunk,WriteTMillis,WriteCPUMicro,ChunkTMillis,ChunkCPUMicro,Reads,ReadErr,Processes,ProcessErr";
		
		/**
		 * We're writing to a .csv file, so produce the data separated by commas
		 * @return The Chunk data as comma-separated-values
		 */
		public String toCSV() {
			
			/* The next three lines are to avoid used-before-set warnings */
			Instant temp = Instant.now();
			Duration chunk = Duration.between(temp, temp);
			Duration write = Duration.between(temp, temp);
			
			if ((chunkStart!=null)&&(chunkEnd!=null))
			    chunk = Duration.between(chunkStart, chunkEnd);
			if ((writeStart!=null)&&(writeEnd!=null))
			    write = Duration.between(writeStart, writeEnd);
			
			String retVal;
			
			if (!cpuData.isAvailable) {
				retVal = new String(chunkNumber+","+
		                  write.toMillis()+","+
				          chunk.toMillis()+","+
		                  readCount+","+
				          readErrorCount+","+
		                  processCount+","+
				          processErrorCount);
			} else {
				long writeCPU = writeCPUEnd - writeCPUStart;
				long chunkCPU = chunkCPUEnd - chunkCPUStart;
				retVal = new String(chunkNumber+","+
		                  write.toMillis()+","+
						  writeCPU+","+
				          chunk.toMillis()+","+
						  chunkCPU+","+
		                  readCount+","+
				          readErrorCount+","+
		                  processCount+","+
				          processErrorCount);
			}

			return retVal; 			          
		}
		
	}

	/**
	 * On some platforms it may be possible to get CPU data from the thread to include in our metrics.
	 * This class manages the access to that data, if we can do it.
	 */
	public class CPUData {
		
		/** Can we get CPU data? */
		private boolean isAvailable;
		/** On z/OS, the Java Class used for access */
		private Class<ZUtil> zUtilClass;
		/** On z/OS, the Method used to get CPU data */
		private Method getCpu;
		
		/**
		 * Constructor
		 * The point here is to try to figure out if we can get to CPU data on the current platform.
		 * If we can, set up whatever we need and set the isAvailable boolean so we know.
		 * On z/OS we look for the com.ibm.jzos.ZUtil.getCpuTimeMicros method.  
		 * Any exceptions or problems, just decide we can't get to it.  
		 */
		public CPUData() {
			try {
				zUtilClass = (Class<ZUtil>) Class.forName("com.ibm.jzos.ZUtil");
				getCpu =zUtilClass.getMethod("getCpuTimeMicros");
				isAvailable=true;
			}
			catch (ClassNotFoundException cnfe) {
				isAvailable=false;
			} catch (NoSuchMethodException e) {
				isAvailable=false;
			} catch (SecurityException e) {
				isAvailable=false;
			}			
		}
		
		/**
		 * Can we get to CPU data?
		 * @return true if we can, false if we can't.
		 */
		public boolean isDataAvailable() {
			return isAvailable;
		}
		
		/**
		 * If we can get CPU data, go get it!
		 * Anything bad that happens, just return a zero.  
		 * If we can't get CPU data (which you can tell, so why are you asking?) return a zero.
		 * @return CPU time used by the current thread, right now.  Or zero.
		 */
		public long getCPUTime() {
			long retVal=0;
			if (isAvailable) {
				try {
					retVal = (long) getCpu.invoke(null);
				}
				catch (IllegalAccessException iae) {} 
				catch (IllegalArgumentException e) {}
				catch (InvocationTargetException e) {}
			}
			return retVal;
		}
		
	}
	
}
