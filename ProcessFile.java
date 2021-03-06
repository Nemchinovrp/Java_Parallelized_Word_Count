import java.io.*;
import java.util.*;

/**
 * Created by Stephanie on 11/8/2015.
 */
public class ProcessFile implements Runnable {

    private final ArrayList<String> list;
    private final int num;
    private final File output;
    private final HashMap<String, Integer> hashMap;
    private final File f;


    public ProcessFile(ArrayList<String> list, int num, File output, HashMap<String, Integer> hashMap, File f) {
        this.list = list;
        this.num = num;
        this.output = output;
        this.hashMap = hashMap;
        this.f = f;
    }

    @Override
    public void run() {
        try {
            for (String line: list) {
                String[] words = line.split("\\s+");
                for (String w : words) {
                    // remove non characters around word for cleaning up word
                    w = w.replaceAll("\\W+", "");
                    // remove white space around word
                    w = w.trim();
                    // all words should be in lower case in output files
                    w = w.toLowerCase();
                    // don't include empty strings
                    if(!w.isEmpty()) {
                        if (hashMap.containsKey(w)){
                            hashMap.put(w, hashMap.get(w) + 1);
                        } else {
                            hashMap.put(w, 1);
                        }
                    }
                }
            }
        } catch (Exception e) {
           e.printStackTrace();
        }

        Map sortedResults = new LinkedHashMap<String, Integer>();
        sortedResults = sortByValue(hashMap);

        FileWriter fwriter;
        BufferedWriter bwriter;
        int size = sortedResults.size();
        int count = 0;

        // create and write to the correct chunk file
        try {
            fwriter = new FileWriter(new File(output, f.getName().toLowerCase() + "_" + num + ".chunk"));
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
            System.out.println("No such file/directory: <" + f.getName() + ">");
            System.exit(0);
        }
    }

    // sort output in descending order (words with largest counts first)
    public Map sortByValue(HashMap<String, Integer> map) {
        synchronized (this) {
            List list = new LinkedList(map.entrySet());
            Collections.sort(list, new Comparator() {
                public int compare(Object count1, Object count2) {
                    return ((Comparable) ((Map.Entry) (count1)).getValue())
                            .compareTo(((Map.Entry) (count2)).getValue());
                }
            });

            Map result = new LinkedHashMap();
            for (Iterator it = list.iterator();
                 it.hasNext(); ) {
                Map.Entry entry = (Map.Entry) it.next();
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }
    }

}