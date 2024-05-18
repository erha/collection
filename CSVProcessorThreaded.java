import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CSVProcessorThreaded {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java CSVProcessorThreaded <input_csv_file>");
            System.exit(1);
        }

        String inputFile = args[0];

        long startTime = System.nanoTime();

        Properties config = readConfig();
        
        MyState myState = new MyState(inputFile, config);
        
        //rawDataQueue, processedDataQueue, header, readCount

        Thread readerThread = new Thread(new CSVReader(myState));
        // semaphore, rawDataQueue, processedDataQueue, header, processCount, columnsToProcess
        Thread[] processThreads = new Thread[myState.threadCount];
        
        for (int i = 0; i < myState.threadCount; i++)
        {
          processThreads[i] = new Thread(new DataProcessor(myState, i + 1));
        }

        // processedDataQueue, writeCount
        Thread writerThread = new Thread(new DataWriter(myState));

        readerThread.start();
        
        for (int i = 0; i < myState.threadCount; i++)
        {
          processThreads[i].start();
        }      
        
        writerThread.start();

        try {
            readerThread.join();
 
            myState.waitForProcessToComplete();
            myState.processedDataQueue.put(new String[]{"EXIT"});
            
            writerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;

        System.out.println("Read count: " + myState.readCount.get());
        System.out.println("Read time: " + (myState.readElapsed / 1_000_000) + " milliseconds");

        System.out.println("Process count: " + myState.processCount.get());
        System.out.println("Process time: " + (myState.getProcessElapsed() / 1_000_000) + " milliseconds");

        System.out.println("Write count: " + myState.writeCount.get());
        System.out.println("Write time: " + (myState.writeElapsed / 1_000_000) + " milliseconds");

        System.out.println("Total execution time: " + (elapsedTime / 1_000_000) + " milliseconds");
    }

    private static Properties readConfig() {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("config.ini")) {
            properties.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}

class MyState {
    public static int COUNT_THREAD_PROCESS = 3;
  
    public BlockingQueue<String[]> rawDataQueue = new LinkedBlockingQueue<>();
    public BlockingQueue<String[]> processedDataQueue = new LinkedBlockingQueue<>();
    public AtomicInteger readCount = new AtomicInteger(0);
    public AtomicInteger processCount = new AtomicInteger(0);
    public AtomicInteger writeCount = new AtomicInteger(0);
    public List<String> columnsToProcess = new ArrayList<String>();
    private Semaphore semaphore = new Semaphore(0);
    public String inputFile;
    public Properties config;
    public String[] headers;
    public long readElapsed;
    private long processElapsed = 0;
    public long writeElapsed;
    public int threadCount = COUNT_THREAD_PROCESS;
  
    public MyState(String inputFile, Properties config) {
      //this.rawDataQueue = new LinkedBlockingQueue<>();
      //this.processedDataQueue = new LinkedBlockingQueue<>();
      //this.readCount = new AtomicInteger(0);
      //this.processCount = new AtomicInteger(0);
      //this.writeCount = new AtomicInteger(0);
      this.inputFile = inputFile;
      this.config = config;

      String columnsString = config.getProperty("Columns");
      String[] columnsArray = columnsString.split(",");
      
      this.columnsToProcess.addAll(Arrays.asList(columnsArray));            

      try
      {
          this.threadCount = Integer.parseInt(config.getProperty("ThreadCount"));
      }
      catch (NumberFormatException e) {
          this.threadCount = COUNT_THREAD_PROCESS;
      }
    }
    
    public void waitForProcessToComplete() throws InterruptedException
    {
      this.semaphore.acquire(threadCount);
    }

    public void releaseProcess()
    {
      this.semaphore.release();
    }
    
    public void setProcessElapsed(long elapsed)
    {
        if (elapsed > processElapsed)
        {
            this.processElapsed = elapsed;
        }
    }

    public long getProcessElapsed()
    {
        return this.processElapsed;
    }
}

class CSVReader implements Runnable {
    private MyState myState;
    private long startTime;

    public CSVReader(MyState myState) {
        this.myState = myState;
        this.startTime = System.nanoTime();
    }

    @Override
    public void run() {
        try (BufferedReader br = new BufferedReader(new FileReader(this.myState.inputFile))) {
            String line;
            this.myState.headers = null;
            while ((line = br.readLine()) != null) {
                String[] data = parseCSVLine(line);
                if (this.myState.headers == null) {
                    this.myState.headers = data;
                    this.myState.processedDataQueue.put(data);
                } 
                else
                {
                    this.myState.rawDataQueue.put(data);
                    this.myState.readCount.incrementAndGet();
                }
            }
            // Add marker to indicate completion
            this.myState.rawDataQueue.put(new String[]{"EXIT"});
            
            long endTime = System.nanoTime();
            this.myState.readElapsed = endTime - this.startTime;

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String[] parseCSVLine(String line) {
        // Implement CSV parsing taking care of quotes
        return line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    }
}

class DataProcessor implements Runnable {
    private long startTime;
    private int count;
    private MyState myState;
    private int instanceNo;

    public DataProcessor(MyState myState, int instanceNo) {
      this.myState = myState;
      this.startTime = System.nanoTime();
      this.count = 0;
      this.instanceNo = instanceNo;
    }

    @Override
    public void run() {
        try {
            while (true) {
                try {
                    String[] data = this.myState.rawDataQueue.take();
                    if (data[0].equals("EXIT")) {
                        // Add marker back to rawDataQueue to allow other threads to exit
                        this.myState.rawDataQueue.put(data);
                        System.out.println("Processing #" + this.instanceNo + " thread exiting");
                        break;
                    }
                    String[] processedData = processData(data);
                    if (processedData != null) {
                        this.myState.processedDataQueue.put(processedData);
                        this.myState.processCount.incrementAndGet();
                        this.count++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.myState.releaseProcess();
        }
  
        long endTime = System.nanoTime();
        this.myState.setProcessElapsed(endTime - this.startTime);
    }

    private String[] processData(String[] data) {
        // Process data according to configuration
        // Use config to determine which columns to process
        String[] processedRow = new String[data.length];
    
        for (int i = 0; i < data.length; i++) {
            if (this.myState.columnsToProcess.contains(this.myState.headers[i])) {
                data[i] = data[i].toUpperCase();
            } else {
                data[i] = data[i];
            }
        }
        
        return data;
    }
}

class DataWriter implements Runnable {
    private MyState myState;
    private long startTime;

    public DataWriter(MyState myState) {
        this.myState = myState;
        this.startTime = System.nanoTime();
    }

    @Override
    public void run() {
        try (FileWriter writer = new FileWriter("output.csv")) {
            int count = 0;
            while (true) {
                String[] data = this.myState.processedDataQueue.take();
                if (data[0].equals("EXIT")) {
                    break; // Exit loop when marker is encountered
                }
                count = count + 1;
                for (int i = 0; i < data.length; i++) {
                    if (i > 0) {
                        writer.append(",");
                    }
                    if (data[i].contains(",") && data[i].startsWith("\"") == false && data[i].endsWith("\"") == false) {
                        writer.append("\"").append(data[i]).append("\"");
                    } else {
                        writer.append(data[i]);
                    }
                }
                writer.append("\r\n");
                if (count != 1) this.myState.writeCount.incrementAndGet();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.nanoTime();
        this.myState.writeElapsed = endTime - this.startTime;
    }
}
