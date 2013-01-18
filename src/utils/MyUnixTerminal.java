package utils;

import jline.Terminal;
import jline.UnixTerminal;

public class MyUnixTerminal extends UnixTerminal {
    private static Terminal term;

    @Override
    protected void checkBackspace() {
        // do not change backspaceDeleteSwitched
        // see: UnixTerminal.checkBackspace()
    }

    // see: Terminal.getTerminal()
    public static Terminal getTerminal() {
        if (term != null) {
            return term;
        }

        term = new MyUnixTerminal();

        try {
            term.initializeTerminal();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return term;
    }
}
