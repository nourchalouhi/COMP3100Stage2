import java.io.*;
import java.net.*;
import java.util.*;

public class TCPClient {

  public static void main(String[] args) {
    Socket s = null;
    BufferedReader in = null;
    DataOutputStream dout = null;
    String reply = " ";
    Boolean flag = true;
    String largestType = null;
    int largestCore = 0;
    int serverCount = 0;
    int sendingTo = 0;
    String[] temp_data = null;
    String[] jobInfo = null;
    int jobID = 0;
    String schd = null;int queueSize= 0;


    try {
      // Connecting to server
      s = new Socket("127.0.0.1", 50000);
      in = new BufferedReader(new InputStreamReader(s.getInputStream()));
      dout = new DataOutputStream(s.getOutputStream());

      dout.write(("HELO\n").getBytes());
      dout.flush();
      System.out.println("SENT: HELO");

      String str = (String) in.readLine();
      System.out.println("RCVD: " + str);

      String username = System.getProperty("user.name");
      dout.write(("AUTH " + username + "\n").getBytes()); // get username from the system
      dout.flush();
      System.out.println("SENT: AUTH");
      reply = in.readLine();
      System.out.println("RCVD: " + reply);

      dout.write("REDY\n".getBytes());
      System.out.println("SENT: REDY");
      dout.flush();
      reply = in.readLine();
      System.out.println("RCVD: " + reply);

      // Loop through jobs
      while (!reply.equals("NONE")) {
        jobInfo = reply.split(" ");

        if (jobInfo[0].equals("JOBN")) {
          jobID = Integer.parseInt(jobInfo[2]);

          if (flag) {
            dout.write("GETS All\n".getBytes());
            dout.flush();
            System.out.println("SENT: GETS All");

            reply = in.readLine(); // DATA X Y
            System.out.println("RCVD: " + reply);
            // Parsing DATA message to get server information
            String[] data = reply.split(" ");
            int numOfServers = Integer.parseInt(data[1]);

            dout.write("OK\n".getBytes());
            dout.flush();

            // Loop through servers to find largest type and core count //incomplete
            for (int i = 0; i < numOfServers; i++) {
              reply = in.readLine();
              System.out.println("RCVD: " + reply);
              temp_data = reply.split(" ");

              if (Integer.parseInt(temp_data[4]) > largestCore) {
                largestCore = Integer.parseInt(temp_data[4]);
                largestType = temp_data[0];
                serverCount = 1;
              } else if (largestType.equals(temp_data[0])) {
                serverCount = serverCount + 1;
              }
            }

            dout.write("OK\n".getBytes());
            System.out.println("SENT: OK");

            dout.flush();
            reply = in.readLine();
            System.out.println(reply); // .
          }
          flag = false;

          
          
          //  Attempt Find server with the shortest queue - incomplete
//           int shortest = Integer.MAX_VALUE;
//           String target = "";
//           for (int i= 0; i< numOfServers; i++) {
//             String[] serverInfo= server.split(" ");
//             
         //queuesize= Integer.parseInt(serverInfo[5]);
//             if (queueSize< shortest) {
//               shortest= queueSize;
//               target= serverInfo;
//             }
//           }
          schd = "SCHD " + jobID + " " + largestType + " " + sendingTo + "\n";
          dout.write(schd.getBytes());
          dout.flush();
          System.out.println(schd);
          reply = in.readLine();
          System.out.println(reply);
          sendingTo++;
          sendingTo = sendingTo % serverCount; //Round Robin

          dout.write("REDY\n".getBytes());
          System.out.println("SENT: REDY");
          dout.flush();
          reply = in.readLine();
          System.out.println("RCVD:" + reply);
        } else {
          dout.write("REDY\n".getBytes());
          System.out.println("SENT: REDY");
          dout.flush();
          reply = in.readLine();
          System.out.println("RCVD:" + reply);
        }
      }
      // Send quit message
      dout.write("QUIT\n".getBytes());
      reply = in.readLine();
      dout.flush();
      System.out.println(reply);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        in.close();
        dout.close();
        s.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
