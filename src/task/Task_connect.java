package task;

import java.io.IOException;
import java.net.InetAddress;

import utils.Utils;

public class Task_connect extends TaskBase {
    @Override
    protected String description() {
        return "show current quorums or set new quorums temporarily\n" +
               "\n" +
               "** NOTE: for permanent change of quorums, modify hosts file\n" +
               "[windows: C:\\Windows\\System32\\drivers\\etc\\hosts / linux: /etc/hosts]\n" +
               "or change value of 'hbase.zookeeper.quorum' in conf/hbase-site.xml";
    }

    @Override
    protected String usage() {
        return "connect [quorums_separated_by_comma]";
    }

    @Override
    public String example() {
        return "connect 172.17.1.206";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return argNumber == 0 || argNumber == 1;
    }

    @Override
    protected void assignParam(String[] args) {
        if (args.length == 1) {
            levelParam.put(Level.OTHER, args[0]);
        }
    }

    @Override
    public void execute()
    throws IOException {
        String quorums = (String) levelParam.get(Level.OTHER);

        if (quorums == null) {
            outputQuorums();
        } else {
            setQuorums(quorums);
        }
    }

    private void outputQuorums()
    throws IOException {
        String quorums = Utils.getQuorums();
        log.info("Current quorums: " + quorums);

        for (String quorum : quorums.split(",")) {
            InetAddress addr = InetAddress.getByName(quorum);
            log.info(String.format(" - %s/%s", addr.getHostName(), addr.getHostAddress()));
        }
    }

    private void setQuorums(String quorums)
    throws IOException {
        Utils.setQuorums(quorums);
        outputQuorums();
    }
}
