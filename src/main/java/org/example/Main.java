package org.example;

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
        String pathOut = args[1];

        System.out.println(new Date());

        // список с коллекциями-значениями колонок, под каждый индекс колонки - отдельная мапа
        // в мапе - значение строки в колонке со ссылкой на строку, в которой значение в последний раз встречалось
        List<Map<String, Integer>> columns = new ArrayList<>();

        // список с множествами групп, в группах - индексы строк
        List<Set<Integer>> groups = new ArrayList<>();

        // список строк
        List<String> allRows = new ArrayList<>();
        int currentRowIndex = 0;

        // список уникальных строк
        Set<String> uniqueRows = new HashSet<>();

        try (BufferedReader reader = readFile(pathIn)){

            String line = reader.readLine();
            while (line != null) {

//                System.out.println(currentRowIndex);

                if (uniqueRows.contains(line)) {
                    line = reader.readLine();
                    continue;
                } else uniqueRows.add(line);

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

                    if (map.containsKey(subString)) {
                        Integer groupIndex = createPair(map.get(subString), currentRowIndex, groups);
                        groupsAdded.add(groupIndex);
                    }
                    map.put(subString, currentRowIndex);
                }

                if (groupsAdded.size() > 1) {
                    mergeGroups(groupsAdded, groups);
                }

                line = reader.readLine();
                currentRowIndex++;

            }

            groups.sort((o1, o2) -> o2.size() - o1.size());
            printFile(groups, allRows, pathOut);


        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Done");
        System.out.println(new Date());

    }

    private static Integer createPair(Integer first, Integer second, List<Set<Integer>> groups) {

        for (int i = 0; i < groups.size(); i++) {
            if (groups.get(i).contains(first)){
                groups.get(i).add(second);
                return i;
            }
        }
        groups.add(new HashSet<>(Arrays.asList(first, second)));
        return groups.size() - 1;
    }

    private static void mergeGroups(Set<Integer> groupsAdded, List<Set<Integer>> groups) {

        List<Integer> groupsAddedList = new ArrayList<>(groupsAdded.stream().toList());
        groupsAddedList.sort((o1, o2) -> o1 - o2);
        Integer groupIndex = groupsAddedList.get(0);
        Set<Integer> sourceGroup = groups.get(groupIndex);

        for (int i = 1; i < groupsAdded.size(); i++) {
            groupIndex = groupsAddedList.get(i);
            Set<Integer> currentGroup = groups.get(groupIndex);
            sourceGroup.addAll(currentGroup);
        }

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