import java.io.*;
import java.net.*;

public class TCPClient {

    public static void main(String[] args) {
        Socket socket = null;
        BufferedReader bufferedReader = null;
        DataOutputStream dataOutputStream = null;
        String reply;
        boolean firstJob = true;
        String largestType = null;
        int largestCoreCount = 0;
        int serverCount = 0;
        int roundRobinCounter = 0;

        try {
            socket = new Socket("127.0.0.1", 50000);
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            dataOutputStream = new DataOutputStream(socket.getOutputStream());

            // Initial handshake with the server
            sendMessage(dataOutputStream, "HELO");
            System.out.println("RCVD: " + readMessage(bufferedReader));

            // Authentication
            String username = System.getProperty("user.name");
            sendMessage(dataOutputStream, "AUTH " + username);
            System.out.println("RCVD: " + readMessage(bufferedReader));

            // Ready for job
            sendMessage(dataOutputStream, "REDY");

            while (!(reply = readMessage(bufferedReader)).equals("NONE")) {
                System.out.println("RCVD: " + reply);

                // If job is received
                if (reply.startsWith("JOBN")) {
                    if (firstJob) {
                        sendMessage(dataOutputStream, "GETS All");
                        reply = readMessage(bufferedReader);
                        System.out.println("RCVD: " + reply);

                        // Handling server list
                        handleServerList(bufferedReader, dataOutputStream);

                        // Reset firstJob flag after handling first job
                        firstJob = false;
                    }

                    // Schedule job to server (round-robin for simplicity)
                    String scheduleCommand = "SCHD " + extractJobId(reply) + " " + largestType + " " + roundRobinCounter;
                    sendMessage(dataOutputStream, scheduleCommand);
                    System.out.println("RCVD: " + readMessage(bufferedReader));

                    roundRobinCounter = (roundRobinCounter + 1) % serverCount;
                    sendMessage(dataOutputStream, "REDY");
                }
            }

            // Quitting
            sendMessage(dataOutputStream, "QUIT");
            System.out.println("RCVD: " + readMessage(bufferedReader));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null) bufferedReader.close();
                if (dataOutputStream != null) dataOutputStream.close();
                if (socket != null) socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void sendMessage(DataOutputStream out, String message) throws IOException {
        out.write((message + "\n").getBytes());
        out.flush();
        System.out.println("SENT: " + message);
    }

    private static String readMessage(BufferedReader in) throws IOException {
        return in.readLine();
    }

    private static int extractJobId(String jobnMessage) {
        return Integer.parseInt(jobnMessage.split(" ")[2]);
    }

    private static void handleServerList(BufferedReader in, DataOutputStream out) throws IOException {
        sendMessage(out, "OK"); // Acknowledge the server list
        sendMessage(out, "OK"); // Finalize server list handling
        System.out.println("RCVD: " + readMessage(in)); // Expecting '.'
    }
}

