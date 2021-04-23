package org.reactome.server.graph.utils;

public class ProgressBarUtils {

    private static final int WIDTH = 70;

    /**
     * Simple method that prints a progress bar to command line
     *
     * @param current Number of entries added to the graph
     */
    public static void updateProgressBar(int current, int total) {
        if (current == total || (current > 0 && current % 100 == 0)) {
            String format = "\r        Database import: %3d%% %s %c";
            char[] rotators = {'|', '/', '—', '\\'};
            double percent = (double) current / total;
            StringBuilder progress = new StringBuilder(WIDTH);
            progress.append('|');
            int i = 0;
            for (; i < (int) (percent * WIDTH); i++) progress.append("=");
            for (; i < WIDTH; i++) progress.append(" ");
            progress.append('|');
            System.out.printf(format, (int) (percent * 100), progress, rotators[((current - 1) % (rotators.length * 100)) / 100]);
        }
    }

    /**
     * Finish progress bar
     */
    public static void completeProgressBar(int total) {
        updateProgressBar(total, total);
    }
}
