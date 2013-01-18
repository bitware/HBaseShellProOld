package utils;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import task.TaskBase;
import task.TaskBase.TaskType;

import jline.Completor;
import jline.ConsoleReader;
import jline.CursorBuffer;
import jline.History;
import jline.SimpleCompletor;

public class MyConsoleReader extends ConsoleReader {
    private static final String FILE_ENCODING   = System.getProperty("file.encoding");
    private static final String OUTPUT_ENCODING = System.getProperty("jline.WindowsTerminal.output.encoding", FILE_ENCODING);

    private static final long MIN_INTERVAL_OF_NORMAL_TWO_KEY_PRESS = 30;

    private final Map<Character, Long> times = new HashMap<Character, Long>();

    public MyConsoleReader()
    throws IOException {
        super(new FileInputStream(FileDescriptor.in),
              new PrintWriter(new OutputStreamWriter(System.out, OUTPUT_ENCODING)),
              null,
              MyUnixTerminal.getTerminal());

        addCompletor();
        addSpecialKeyHandler();
    }

    private void addCompletor() {
        Map<String, TaskType> aliasMap = TaskBase.getAliasMap();

        String[] aliases = new String[aliasMap.size()];
        int i = 0;

        for (Object alias : aliasMap.keySet().toArray()) {
            aliases[i++] = (String) alias;
        }

        Completor completor = new SimpleCompletor(aliases);
        addCompletor(completor);
    }

    private void addSpecialKeyHandler() {
        addTriggeredAction('1', new SpecialKeyPressed('1'));
        addTriggeredAction('2', new SpecialKeyPressed('2'));
        addTriggeredAction('4', new SpecialKeyPressed('4'));
        addTriggeredAction('5', new SpecialKeyPressed('5'));
        addTriggeredAction('6', new SpecialKeyPressed('6'));
        addTriggeredAction('~', new KeyWavePressed());
    }

    // SpecialKeyPressed handler
    class SpecialKeyPressed implements ActionListener {
        private final char ch;

        public SpecialKeyPressed(char ch) {
            this.ch = ch;
            times.put(ch, 0L);
        }

        @Override
        public void actionPerformed(ActionEvent e) {
            times.put(ch, System.currentTimeMillis());

            try {
                putString(Character.toString(ch));
            } catch (IOException e1) {
                RootLog.getLog().error(null, e1);
            }
        }
    }

    // KeyWavePressed handler
    class KeyWavePressed implements ActionListener {
        private static final char ch = '~';

        @Override
        public void actionPerformed(ActionEvent e) {
            long time = System.currentTimeMillis();

            try {
                if (isOneKey(time, '1')) {               // "1~" --> Home key pressed
                    gotoBeginOfLine();
                } else if (isOneKey(time, '2')) {        // "2~" --> Insert key pressed
                    // do nothing
                } else if (isOneKey(time, '4')) {        // "4~" --> End key pressed
                    gotoEndOfLine();
                } else if (isOneKey(time, '5')) {        // "5~" --> Page up key pressed
                    moveToFirstHistoryEntry();
                } else if (isOneKey(time, '6')) {        // "6~" --> Page down key pressed
                    moveToLastHistoryEntry();
                } else {
                    putString(Character.toString(ch));
                }
            } catch (IOException e1) {
                RootLog.getLog().error(null, e1);
            }
        }

        private boolean isOneKey(long time, char ch)
        throws IOException {
            long diff = time - times.get(ch);

            if (diff >= 0 && diff < MIN_INTERVAL_OF_NORMAL_TWO_KEY_PRESS / 2) {
                backspace();
                return true;
            }

            return false;
        }

        private void gotoBeginOfLine()
        throws IOException {
            setCursorPosition(0);
        }

        private void gotoEndOfLine()
        throws IOException {
            CursorBuffer buf = getCursorBuffer();
            moveCursor(buf.length() - buf.cursor);
        }

        private void moveToFirstHistoryEntry()
        throws IOException {
            History history = getHistory();

            if (history.moveToFirstEntry()) {
                setBuffer(history.current());
            }
        }

        private void moveToLastHistoryEntry()
        throws IOException {
            History history = getHistory();

            if (history.moveToLastEntry()) {
                setBuffer(history.current());
            }
        }

        private void setBuffer(String string)
        throws IOException {
            CursorBuffer buf = getCursorBuffer();

            if (!string.equals(buf.toString())) {
                gotoBeginOfLine();
                killLine();                             // clear to the end of the line
                putString(string);
            }
        }
    }
}
