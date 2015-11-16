/**
 * Created by Stephanie on 10/16/2015
 * Assignment 3: Parallelized Word Count
 * CS3250 UVU Fall 2015
 * Last Modified: 11/15/2015
 *
 * Objective: Implement a word counting program that computes word counts in parallel,
 * merging the results in the end. Program will break down its tasks into smaller pieces
 * by working on different parts of a text file or text file(s), computing word counts
 * for the smaller pieces and then merging the count of words at the end.
 *
 * Example Input: java WordCount c:\Users\test.txt 100 5
 * First argument = current directory
 * Second argument = chunk size, meaning the size of each task defined by the number of lines.
 * In this case, 100 lines per task. Each thread can handle a maximum of 100 lines at a time.
 * Third argument = number of threads to use; in this case 5
 *
 * Output: would contain a list of chunk files in the output/ folder along with 1 results.txt
 * file in the same output folder. Chunk files would contain word counts for the smaller pieces
 * and results.txt would contain overall word counts based on all the chunks.
 */

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

// one main class WordCount.java with NO PACKAGE
public class WordCount {

    public static void main(String args[]) {
        // If wrong number of input arguments specified or invalid arguments (wrong number of threads, wrong chunk size etc), then print the following:
        // Usage: java WordCount <file|directory> <chunk size 10-5000> <num of threads 1-100>
        if(args.length == 0) {
            wrongInput();
        }

        String fileOrDirc = args[0];
        int maxLinesInChunk = 0;
        int numOfThread = 0;
        // Make sure the input has exactly 3 arguments
        if (args.length == 3) {
            // check that the first argument is a file or directory
            fileOrDirc = args[0];
            if(!checkFileDirc(fileOrDirc)) {
                noSuchFile(fileOrDirc);
            }
            // check that the second argument is setting chunk size between 10-5000
            if (isInt(args[1])) {
                maxLinesInChunk = Integer.parseInt(args[1]);
                if (maxLinesInChunk < 10 || maxLinesInChunk > 5000) {
                    wrongInput();
                }
            }
            else {
                wrongInput();
            }

            // check that the third argument is setting the thread size to 1-100
            if (isInt(args[2])) {
                numOfThread = Integer.parseInt(args[2]);
                if (numOfThread < 1 || numOfThread > 100) {
                    wrongInput();
                }
            }
            else {
                wrongInput();
            }
        }
        else {
            wrongInput();
        }

        // If cannot create output directory print: "Cannot create output directory, please try again" and terminate program
        File output = new File("output/");
        createDirectory(output);
        File file = new File(fileOrDirc);
        // If the input file is a directory search every file in the directory
        if(file.isDirectory()){
            ExecutorService service = Executors.newFixedThreadPool(numOfThread);

            File[] filesInDirectory = file.listFiles();
            for(File f: filesInDirectory) {

                try {
                    FileReader fr = new FileReader(f);
                    Scanner scanner = new Scanner(fr);
                    int count = 0;
                    int i = 0;
                    String line;
                    ArrayList<String> al = new ArrayList<String>(maxLinesInChunk);
                    HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
                    while (scanner.hasNext()) {
                        line = scanner.nextLine();
                        al.add(line);
                        count++;
                        if (count == maxLinesInChunk || !scanner.hasNext()) {

                            service.execute(new ProcessFile(al, i++, output, hashMap, f));
                            al = new ArrayList<String>(maxLinesInChunk);
                            hashMap = new HashMap<String, Integer>();
                            count = 0;
                        }
                        if (!scanner.hasNext()) {
                            fr.close();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

                service.shutdown();
                try {
                    service.awaitTermination(10, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();

                }
        }

        else {
            // if passed a file just search that file
            ExecutorService service = Executors.newFixedThreadPool(numOfThread);
            try{
                FileReader fr = new FileReader(file);
                Scanner scanner = new Scanner(fr);
                int count = 0;
                int i = 0;
                String line;
                ArrayList<String> al = new ArrayList<String>();
                HashMap<String, Integer> tm = new HashMap<String, Integer>();
                 while(scanner.hasNext()) {
                    line = scanner.nextLine();
                    al.add(line);
                    count++;
                    if (count == maxLinesInChunk || !scanner.hasNext()) {

                        service.execute(new ProcessFile(al, i++, output, tm, file));

                        al = new ArrayList<String>();
                        tm = new HashMap<String, Integer>();
                        count = 0;
                    }
                     if (!scanner.hasNext()) {
                         fr.close();
                     }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            service.shutdown();
            try {
                service.awaitTermination(5000, TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        resultFile(output);
    }

    // Method the create a results file with the sum of all words in chunk files
    public static void resultFile(File output) {
        HashMap<String, Integer> hm = new HashMap<String, Integer>();
        File[] Files = output.listFiles();
        for(File file: Files) {
            try {
                FileReader fr = new FileReader(file);
                Scanner sc = new Scanner(fr);
                String line;
                while (sc.hasNext()) {
                    line = sc.nextLine();
                    String[] words = line.split("\t");
                    String word;
                    int count;
                    if(words.length == 2) {
                        word = words[0];
                        count = Integer.parseInt(words[1]);

                        if (hm.containsKey(word)){
                            hm.put(word, hm.get(word) + count);
                        }
                        else {
                            hm.put(word, count);
                        }
                    }

                    if(!sc.hasNext()){
                        fr.close();
                    }
                }
            } catch(IOException e){
                    e.printStackTrace();
            }
        }

        LinkedHashMap<String, Integer> sortedResults = new LinkedHashMap<String, Integer>();
        sortedResults = sortByValue(hm);

        FileWriter fwriter;
        BufferedWriter bwriter;

        try {
            fwriter = new FileWriter(new File(output, "results.txt"));
            bwriter = new BufferedWriter(fwriter);

            List<Map.Entry<String, Integer>> list = new ArrayList<>(sortedResults.entrySet());
            for(int i = list.size() - 1; i >=0; i--){
                Map.Entry<String, Integer> entry = list.get(i);
                bwriter.write(entry.getKey() + "\t" + entry.getValue());
                bwriter.newLine();
            }
            bwriter.close();
        }
        catch(IOException e) {
            System.out.println("No such file/directory: <" +">");
            System.exit(0);
        }

    }

    // sort the output in descending order, words with largest count to less count
    public static LinkedHashMap<String, Integer> sortByValue(HashMap<String, Integer> map) {
        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> num1, Map.Entry<String, Integer> num2) {
                return ((Comparable<Integer>)((num1)).getValue())
                        .compareTo((num2).getValue());
            }
        });

        LinkedHashMap<String, Integer> result = new LinkedHashMap<String, Integer>();
        for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it.hasNext();) {
            Map.Entry<String, Integer> entry = it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    // create the output directory
    public static void createDirectory(File output) {
        if (!output.exists()){
            boolean result = false;

            try{
                output.mkdir();
                result = true;
            }
            catch(SecurityException se) {
                System.out.println("Cannot create output directory, please try again");
                System.exit(0);
            }
        }
        else {
            // if the directory already exists check for content and delete any that exists
            deleteContent(output);
        }
    }

    // delete any content already in output directory
    public static void deleteContent(File output){
        File[] files = output.listFiles();
        if (files != null) {
            for(File f: files) {
                if(f.isDirectory()){
                    deleteContent(f);
                }
                else {
                    f.delete();
                }
            }
        }
    }

    // check if the argument is a file or directory
    public static boolean checkFileDirc(String fileOrDirc) {
        File f = new File(fileOrDirc);
        if ((f.exists() && !f.isDirectory()) || ((f.exists() && f.isDirectory()))) {
            return true;
        }
        else {
            return false;
        }
    }

    // check if the argument is an integer
    public static boolean isInt(String string) {
        try {
            Integer.parseInt(string);
            return true;
        }
        catch (NumberFormatException e) {
            return false;
        }
    }

    // output for wrong input
    public static void wrongInput() {
        System.out.println("Usage: java WordCount <file|directory> <chunk size 10-5000> <num of threads 1-100>");
        System.exit(0);
    }

    // output for file or directory that does not exist
    public static void noSuchFile(String fileOrDirc) {
        System.out.println("No such file/directory: <" + fileOrDirc +">");
        System.exit(0);
    }

}