package task;

import java.io.IOException;

import utils.Utils;
import utils.Utils.FoundLine;

import main.HBShell;

public class Task_history extends TaskBase {
    private int    count   = (int)(HBShell.defaultHistoryCount + 0);
    private int    cnt     = 0;
    private String pattern = null;

    private String[] cmds = null;

    @Override
    protected String description() {
        return "show command history";
    }

    @Override
    protected String usage() {
        return "history [count [pattern]]";
    }

    @Override
    public String example() {
        return "history 3 get";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber >= 0 && argNumber <= 2;
    }

    @Override
    protected void assignParam(String[] args) {
        if (args.length >= 1) {
            this.count = Integer.valueOf(args[0]);
        }

        if (args.length >= 2) {
            this.pattern = args[1];
        }

        this.cmds = new String[count];

        // just for showing parameters
        levelParam.put(Level.OTHER, Utils.join(args, ", "));
    }

    @Override
    public void execute() {
        // define found line handler
        FoundLine foundLine = new FoundLine() {
            @Override
            public boolean foundLine(String cmd) {
                if (cmd.equals("")) {
                    return false;
                }

                if (pattern == null || Utils.isMatch(cmd, pattern)) {
                    cmds[count - cnt - 1] = cmd;
                    cnt++;

                    if (cnt == count) {
                        return true;    // all required cmds found, return true to end search
                    }
                }

                return false;
            }
        };

        // search history file from end
        try {
            Utils.searchFileFromEnd(HBShell.HISTORY_FILE, foundLine);
        } catch (IOException e) {
            // OK, history file not found
            return;
        }

        // output
        for (String cmd : cmds) {
            if (cmd == null) {
                continue;
            }

            log.info(cmd);
        }
    }

    public static String getLastCmd() {
        final String[] lastCmd = new String[1];

        // define found line handler
        FoundLine foundLine = new FoundLine() {
            @Override
            public boolean foundLine(String cmd) {
                if (!cmd.equals("")) {
                    lastCmd[0] = cmd;
                    return true;    // return true to end search
                }

                return false;
            }
        };

        try {
            Utils.searchFileFromEnd(HBShell.HISTORY_FILE, foundLine);
        } catch (IOException e) {
            // OK, history file not found
        }

        return lastCmd[0];
    }
}
