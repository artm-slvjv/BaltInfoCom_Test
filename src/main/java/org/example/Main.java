package org.example;

import com.sun.source.tree.IfTree;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {

        if (args.length < 2){
            System.out.println("Не указаны параметры-пути входного и результирующего файла");
            return;
        }

        String pathIn = args[0];
//        String pathIn = "ex.txt";
        String pathOut = args[1];

        System.out.println(new Date());

        // список с коллекциями-значениями колонок, под каждый индекс колонки - отдельная мапа
        // в мапе - значение строки в колонке со ссылкой на строку, в которой значение в последний раз встречалось
        List<Map<String, Integer>> columns = new ArrayList<>();

        // список с множествами групп, в группах - индексы строк
        List<Set<Integer>> groups = new ArrayList<>();

        // список строк
        List<String> allRows = new ArrayList<>();
        Set<String> uniqueRows = new HashSet<>();
        int currentRowIndex = 0;
        int ind = 0;
        ExecutorService pool = Executors.newCachedThreadPool();
        List<Callable<Object>> tasks = new ArrayList<>();

        try (BufferedReader reader = readFile(pathIn)){

            String line = reader.readLine();
            while (line != null && ind < 1000000) {

                long time =  System.currentTimeMillis();
//                System.out.println(ind);
                ind++;

                if (uniqueRows.contains(line)) {

                    line = reader.readLine();

                    continue;

                } else
                    uniqueRows.add(line);

                allRows.add(line);
                String[] strings = line.split(";");

                // множество индексов групп, в которые будет добавлена строка
                Set<Integer> groupsAdded = new HashSet<>();

                for (int i = 0; i < strings.length; i++) {

                    String subString = strings[i];

                    Map<String, Integer> map;
                    if (i > columns.size() - 1) {
                        map = new HashMap<>();
                        columns.add(map);
                    } else map = columns.get(i);

                    if (subString.equals("") | subString.equals("\"\"")) continue;

                    int finalCurrentRowIndex = currentRowIndex;
                    tasks.add(new Callable<>() {
                        @Override
                        public Object call() throws Exception {

                            Pair pair = new Pair();
                            if (map.containsKey(subString)) {
                                pair = createPair(map.get(subString), finalCurrentRowIndex, groups);
//                                groupsAdded.add(groupIndex);
                            }

                            map.put(subString, finalCurrentRowIndex);

                            return pair;
                        }
                    });

//                    futures.add(future);

//                    String subString = strings[i];
//
//                    Map<String, Integer> map;
//                    if (i > columns.size() - 1) {
//                        map = new HashMap<>();
//                        columns.add(map);
//                    } else map = columns.get(i);
//
//                    if (subString.equals("") | subString.equals("\"\"")) continue;
//
//                    if (map.containsKey(subString)) {
//                        Integer groupIndex = createPair(map.get(subString), currentRowIndex, groups);
//                        groupsAdded.add(groupIndex);
//                    }
//
//                    map.put(subString, currentRowIndex);

                }

                List<Future<Object>> futures = null;
                try {
                     futures = pool.invokeAll(tasks);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                for (Future future:futures
                     ) {
                    try {
                        Pair pair = (Pair) future.get();

                        if (pair.isEmpty()) continue;

                        if (pair.getFoundGroup() != null) {
                            groups.get(pair.getFoundGroup()).add(pair.getSecondValueIndex());
                            groupsAdded.add(pair.getFoundGroup());
                        } else {
                            groups.add(new HashSet<>(Arrays.asList(pair.getFirstValueIndex(), pair.getSecondValueIndex())));
                            groupsAdded.add(groups.size() - 1);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                }


                if (groupsAdded.size() > 1) {
//                    long time =  System.currentTimeMillis();
                    mergeGroups(groupsAdded, groups);
//                    long time2 =  System.currentTimeMillis();
//                    System.out.println(time2 - time);
                }

                line = reader.readLine();
                currentRowIndex++;
                tasks.clear();

                long time2 =  System.currentTimeMillis();
                System.out.println(ind + " ---- " + (time2 - time));

            }

            groups.sort((o1, o2) -> o2.size() - o1.size());
            printFile(groups, allRows, pathOut);


        } catch (IOException e) {
            e.printStackTrace();
        }

        pool.shutdown();

        System.out.println("Done");
        System.out.println(new Date());

    }

    private static Pair createPair(Integer first, Integer second, List<Set<Integer>> groups) {
//                    long time =  System.currentTimeMillis();

        Pair pair = new Pair();

        for (int i = 0; i < groups.size(); i++) {
            for (Integer element : groups.get(i)
            ) {
                if (element.equals(first)) {
//                    groups.get(i).add(second);

//                    long time2 =  System.currentTimeMillis();
//                    System.out.println(time2 - time);

                    pair.setFoundGroup(i);
                    pair.setSecondValueIndex(second);

                    return pair;
                }
            }
        }
//        groups.add(new HashSet<>(Arrays.asList(first, second)));
//        return groups.size() - 1;

        pair.setFirstValueIndex(first);
        pair.setSecondValueIndex(second);

        return pair;
    }

    private static void mergeGroups(Set<Integer> groupsAdded, List<Set<Integer>> groups) {

        List<Integer> groupsAddedList = new ArrayList<>(groupsAdded.stream().toList());
//        Collections.sort(groupsAddedList);
        groupsAddedList.sort((o1, o2) -> o1 - o2);
        Integer groupIndex = groupsAddedList.get(0);
        Set<Integer> sourceGroup = groups.get(groupIndex);



//        System.out.println("--" + groupsAdded.size());
//        System.out.println("----" + groups.size());

        for (int i = 1; i < groupsAdded.size(); i++) {
            groupIndex = groupsAddedList.get(i);
            Set<Integer> currentGroup = groups.get(groupIndex);
            sourceGroup.addAll(currentGroup);
        }

//        for (int i = 1; i < groupsAdded.size(); i++) {
//            groupIndex = groupsAddedList.get(i);
//            groups.remove((int) groupIndex);
//        }

        for (int i = groupsAdded.size() - 1; i > 0; i--) {
            groupIndex = groupsAddedList.get(i);
            groups.remove((int) groupIndex);
        }


    }

    private static BufferedReader readFile(String pathIn) throws FileNotFoundException {
        File file = new File(pathIn);
        return new BufferedReader(new FileReader(file));
    }

    private static void printFile(List<Set<Integer>> groups, List<String> allRows, String pathOut) throws IOException {

        try (FileWriter file = new FileWriter(pathOut, false)) {
            file.write(String.format("Total groups: %s \n", groups.size()));

            for (int i = 0; i < groups.size(); i++) {
                file.write(String.format("Group %s \n", i));
                for (Integer element : groups.get(i)
                ) {
                    file.write(allRows.get(element) + "\n");
                }
            }
            file.flush();
        }

    }

}