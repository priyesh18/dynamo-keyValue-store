package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.ListIterator;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static String currDevicePort;
	List<String> allKeys = new ArrayList<>();
	List<Avd> activeDevices = new ArrayList<>();
	private static final String[] portList = new String[]{
			REMOTE_PORT4, REMOTE_PORT1, REMOTE_PORT0, REMOTE_PORT2, REMOTE_PORT3,REMOTE_PORT4, REMOTE_PORT1};

	static final int SERVER_PORT = 10000;
	private static Context context;


	public String togglePort(String port) {
		if(port.length() > 4) {
			return String.valueOf((Integer.parseInt(port)/2));
		}
		else return String.valueOf((Integer.parseInt(port)*2));
	}


	class Avd {
		private String port;
		private String hash;
		private String pref1_port;
		private String pref2_port;
		private List<String> pref_list = new ArrayList<>();
		Avd(String port, String pref1, String pref2) {
			this.port = port;
			this.pref1_port = pref1;
			this.pref2_port = pref2;

			this.pref_list.add(this.port);
			this.pref_list.add(this.pref1_port);
			this.pref_list.add(this.pref2_port);

			try {
				this.hash = genHash(togglePort(this.port));

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		public String getPort() {
			return this.port;
		}
		public String getHash() {
			return this.hash;
		}


	}
//	private static Avd currentAvd = null;






	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		context = getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		currDevicePort = myPort;

//		currentAvd = new Avd();
		// create the node ring.
		Avd temp = null;
		for(int i = 0; i < portList.length-2; i++) {
			temp = new Avd(portList[i], portList[i+1], portList[i+2]);
			activeDevices.add(temp);
		}

//		for(Avd a: activeDevices) {
//            Log.v(a.getPort(), "Pref1: "+ a.pref1_port + " Pref2: "+ a.pref2_port);
//        }


		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
//		//replace myPort with getPort;
//		if(!myPort.equals(REMOTE_PORT0)) {
//			String msg = "J,"+portStr+"\n";
//			String recPort = REMOTE_PORT0;
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, recPort);
//		}



		return false;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			try {
				while (true) {
					Socket clientSocket = serverSocket.accept();
					BufferedReader in = new BufferedReader(
							new InputStreamReader(clientSocket.getInputStream()));
					PrintWriter out =
							new PrintWriter(clientSocket.getOutputStream(), true);
					String inputLine = in.readLine();
					String[] str = inputLine.split(",");
					if (str[0].equals("I")) {
						ContentValues cv = new ContentValues();
						cv.put(KEY_FIELD, str[1]);
						cv.put(VALUE_FIELD, str[2]);
						insert(null, cv);
						out.println("I, done insert\n");

					}
					else if(str[0].equals("Q")) {
						Log.d(TAG, "Query request received for:"+str[1]);
						Log.d("server", "key found");
						Cursor c = query(null,null, str[1], new String[]{}, null);
						while(c.moveToNext()) {
							String pair = c.getString(0) +","+ c.getString(1);
							out.println(pair);
						}
						out.println("done");
//                        }
					}
					else if(str[0].equals("R")) {
						Log.d(TAG, "Remove request received for:"+str[1]);
					}
					else {
						Log.e(TAG,"You are sending something wrong!");
					}
				}

			} catch (IOException e) {
				Log.e(TAG, "Exception caught when trying to listen on port "
						+ sockets[0] + " or listening for a connection");
				Log.e(TAG, e.getMessage());
			}


			return null;
		}
	}

	/***
	 * Called by insert, query or remove function
	 * msgs array contains the 'msgtosend' and 'recPort'
	 */
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try {

				String msgToSend = msgs[0];
				String recPort = msgs[1];
				String replyStr = "Empty";

				BufferedReader in;

				Socket socket;
				PrintWriter out;

				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(recPort));

				out = new PrintWriter(socket.getOutputStream(), true);
				out.println(msgToSend);


				//Reply from the server
				in = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));
				replyStr = in.readLine();
				String[] reply = replyStr.split(",");
//				if(reply[0].equals("J")) {
//					currentAvd.pred_port = "10000"; // this port doesn't matter.
//				}

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}


			return null;
		}
	}

	/*
	returns the avd object of partition that should store the message with @params(msg: key)
	 */
	private Avd getAVD(String msg) {
		String msgHash = "";
		try {
			msgHash = genHash(msg);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		ListIterator<Avd> it = activeDevices.listIterator();

		while(it.hasNext()) {
			Avd tempAvd = it.next();
			if(tempAvd.getHash().compareTo(msgHash) > 0) return tempAvd;
		}
		// If not returned yet. The msgHash is greater than the last Avd in the list.
		// Hence the successor will be the first Avd;
		it = activeDevices.listIterator();
		return it.next();

	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if(selection.compareTo("@") == 0) {
			allKeys.clear();
			Log.v(TAG, "Deleting @");
		}
		else if(selection.compareTo("*") == 0) {
			// get the correct port of the msg's location, create a new client and sent the delete query to that port.
			Log.v(TAG, "Deleting from all *");
		}
		else {
			//perform the following line in the responsible avd.
			allKeys.remove(selection);
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String k = values.getAsString("key");
		String v = values.getAsString("value");

		String filename = null;
		if(uri == null) {
			// request came from server insert
			Log.d("From Insert if", "HI");
			try {
				filename = genHash(k);
			}
			catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		else {
			// request received from other activity.
			Avd temp = getAVD(k);
			String port = temp.getPort();
			Log.d("From Insert else ", port);
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "I," + k + "," + v + "\n", port);
			for(String p: temp.pref_list) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "I," + k + "," + v + "\n", p);
			}
			return null;
		}


		String fileContents = v;
//        Log.e(TAG, "inside insert");
		try (FileOutputStream fos = getContext().openFileOutput(filename, Context.MODE_PRIVATE)) {
			fos.write(fileContents.getBytes());
			allKeys.add(k);
		} catch (FileNotFoundException e) {
			Log.e(TAG, "File not found exception in insert");

			e.printStackTrace();
		} catch (IOException e) {
			Log.e(TAG, "IOException in insert");
			e.printStackTrace();
		}
//        Log.v(TAG, values.toString());
		return uri;
	}

	private String queryHelper(String selection) {
		FileInputStream fis = null;
		try {
			fis = getContext().openFileInput(genHash(selection));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
		StringBuilder stringBuilder = new StringBuilder();
		try(BufferedReader reader = new BufferedReader(inputStreamReader)) {
			String line = reader.readLine();
			stringBuilder.append(line).append('\n');
			return line;


		}
		catch (IOException e) {
			// Error occurred when opening raw file for reading.
			Log.e(TAG, "Error while opening file");
		} finally {
//            String contents = stringBuilder.toString();
		}

		return null;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});

		if(selection.compareTo("@") == 0) {
			Log.v(TAG, "Query @");
			for(String k: allKeys) {
				String line = queryHelper(k);
				mc.newRow().add("key", k).add("value",line);
			}
			return mc;
		}
		if(allKeys.contains(selection)) {
			String line = queryHelper(selection);
			mc.newRow().add("key", selection).add("value",line);
			return mc;
		}

		// Needs networking now.
		BufferedReader in;
		Socket socket;
		PrintWriter out;

		if(selection.equals("*")) {
			// Adding current device's keys; doing @
			for(String k: allKeys) {
				String line = queryHelper(k);
				mc.newRow().add("key", k).add("value",line);
			}
			ListIterator<Avd> it = activeDevices.listIterator();
//			String fromPort = selectionArgs == null ? REMOTE_PORT0 : selectionArgs[0];
//                Log.d("Query **", fromPort);
			while(it.hasNext()) {
				Avd temp = it.next();

				Log.d("Query in"," *" );
				if(temp.getPort().equals(currDevicePort)) continue;
				String msgToSend = "Q,@";

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(temp.getPort()));

					out = new PrintWriter(socket.getOutputStream(), true);
//					Log.d("Query *", "sending to:"+temp.getLongPort());
					out.println(msgToSend);
					//Reply from the server
					in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));

					String replyStr = "";
					while(true) {
						replyStr = in.readLine();
						if(replyStr.equals("done")) break;

						Log.v("query2", replyStr);

						String[] kv = replyStr.split(",");
						mc.newRow().add("key", kv[0]).add("value", kv[1]);
					}

//                    socket.close();


				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return mc;
		}
		else {
			Avd temp = getAVD(selection);
			String port = temp.getPort();
			String msgToSend = "Q," + selection;

			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(port));

				out = new PrintWriter(socket.getOutputStream(), true);
				out.println(msgToSend);
				//Reply from the server
				in = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));

				String replyStr = "";
				while(true) {
					replyStr = in.readLine();
					if(replyStr.equals("done")) break;

					Log.d("query3", replyStr);

					String[] kv = replyStr.split(",");
					mc.newRow().add("key", kv[0]).add("value", kv[1]);
				}
				return mc;



			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
