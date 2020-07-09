package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
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
	private static boolean theHack = false;
	private static String currDevicePort;
	private static Avd currDevice;
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
		public String toString() { return "Port:" + this.port +" ,Pref1:"+this.pref1_port+" ,Pref2:"+this.pref2_port; }


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
			if(portList[i].equals(currDevicePort)) currDevice = temp;
			activeDevices.add(temp);
		}
		Log.d("My_port", currDevice.getPort());

//		for(Avd a: activeDevices) {
//            Log.v(a.getPort(), "Pref1: "+ a.pref1_port + " Pref2: "+ a.pref2_port);
//        }
		// Failure Handling
		new FailSafe().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort,null);


		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}




		return false;
	}
	private class FailSafe extends  AsyncTask<String, Void, Void> {

		@Override
		protected  Void doInBackground(String... params) {
			Cursor c = query(null,null, "*", new String[]{}, null);
			while(c.moveToNext()) {
				ContentValues cv = new ContentValues();
				String key = c.getString(0);
				String value = c.getString(1);
				Avd tempAvd = getAVD(key);
//				Log.v("Fail Safe", tempAvd.toString());
				if(tempAvd.getPort().equals(currDevice.pref2_port) || tempAvd.getPort().equals(currDevice.pref1_port)) continue;
				cv.put(KEY_FIELD, key);
				cv.put(VALUE_FIELD, value);
				insert(null, cv);
			}

			return null;
		}
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
						Log.v(TAG, "Query request received for:"+str[1]);
						Cursor c = query(null,null, str[1], new String[]{}, null);
						while(c.moveToNext()) {
							String pair = c.getString(0) +","+ c.getString(1);
//							Log.v("Server Query", pair);
							out.println(pair);
						}
						out.println("done");
//                        }
					}
					else if(str[0].equals("R")) {
						Log.d(TAG, "Remove request received for:"+str[1]);
						delete(null, str[1], new String[]{} );
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
				socket.setTcpNoDelay(true);


				out = new PrintWriter(socket.getOutputStream(), true);
				out.println(msgToSend);


				//Reply from the server
				in = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));
				replyStr = in.readLine();
//				String[] reply = replyStr.split(",");
//				if(reply[0].equals("J")) {
//					currentAvd.pred_port = "10000"; // this port doesn't matter.
//				}

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			} catch (Exception e) {
				e.printStackTrace();
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
		Log.i("Inside Delete", "Uri"+uri);
		String[] fils = getContext().getApplicationContext().fileList();
		theHack = true;

			for(String k: fils) {
				try {
					Log.d("Delete loop for files",""+getContext().getApplicationContext().deleteFile(k));

				} catch (Exception e) {
					e.printStackTrace();
				}

			}
//		}

		allKeys.clear();
		if(uri != null) {
			Avd temp = getAVD(selection);
			for(String p: temp.pref_list) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "R," + selection+ "\n", p);
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public String syncClient(String msg, String port) {
		try {

			String msgToSend = msg;
			String recPort = port;
			String replyStr = "Empty";

			BufferedReader in;

			Socket socket;
			PrintWriter out;

			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(recPort));
			socket.setTcpNoDelay(true);


			out = new PrintWriter(socket.getOutputStream(), true);
			out.println(msgToSend);


			//Reply from the server
			in = new BufferedReader(
					new InputStreamReader(socket.getInputStream()));
			replyStr = in.readLine();
			return replyStr;


		} catch (UnknownHostException e) {
			Log.e(TAG, "ClientTask UnknownHostException");
		} catch (IOException e) {
			Log.e(TAG, "ClientTask socket IOException");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Some error";

	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String k = values.getAsString("key");
		String v = values.getAsString("value");

		String filename = null;
		if(uri == null) {
			// request came from server insert
			Log.v("From Insert if", k);
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
			Log.v("From Insert else ", k);
			for(String p: temp.pref_list) {
//				String reply = syncClient("I," + k + "," + v + "\n", p);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "I," + k + "," + v + "\n", p);
			}
			return null;
		}


		String fileContents = v;
//        Log.e(TAG, "inside insert");
//		synchronized (this) {
			try (FileOutputStream fos = getContext().getApplicationContext().openFileOutput(filename, Context.MODE_PRIVATE)) {
				fos.write(fileContents.getBytes());
				fos.close();
				allKeys.add(k);

			} catch (FileNotFoundException e) {
				Log.e(TAG, "File not found exception in insert");

				e.printStackTrace();
			} catch (IOException e) {
				Log.e(TAG, "IOException in insert");
				e.printStackTrace();
			}
//		}

//        Log.v(TAG, values.toString());
		return uri;
	}

	private String queryHelper(String selection) {
		FileInputStream fis = null;
//		synchronized (this) {
			try {
				fis = getContext().getApplicationContext().openFileInput(genHash(selection));
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
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
//		}


		return null;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});


		if(selection.compareTo("@") == 0) {
			Log.v(TAG, "Query @");
			Log.d("Query @", ""+allKeys.size());
			for(String k: allKeys) {
				String line = queryHelper(k);
				mc.newRow().add("key", k).add("value",line);
			}
			return mc;
		}
		if(theHack) {
			Log.e("The hack", ""+theHack);
			int i = 1000;
			while(i-- > 0);
			theHack = false;
			Log.e("The hack after", ""+theHack);
		}
//		Avd t = getAVD(selection);
		if(allKeys.contains(selection)) {
			String line = queryHelper(selection);
			Log.d("Query in local", selection);
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

				Log.v("Query in"," *" );
				if(temp.getPort().equals(currDevicePort)) continue;
				String msgToSend = "Q,@";

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(temp.getPort()));

					out = new PrintWriter(socket.getOutputStream(), true);
//					Log.d("Query *", "sending to:"+temp.getLongPort());
					out.println(msgToSend);
					Log.d("* query sent to", temp.getPort());
					//Reply from the server
					in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));

					String replyStr = "";
					while(true) {
						replyStr = in.readLine();
						if(replyStr.equals("done")) break;

//						Log.v("query2(*)", replyStr);

						String[] kv = replyStr.split(",");
						mc.newRow().add("key", kv[0]).add("value", kv[1]);
					}

//                    socket.close();


				} catch (IOException e) {
					e.printStackTrace();
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
			return mc;
		}
		else {
			Avd temp = getAVD(selection);
			String port = temp.getPort();
			String msgToSend = "Q," + selection;
			ArrayList<String> rev_list = new ArrayList<>(temp.pref_list);
			Collections.reverse(rev_list);

			for(String p: rev_list) {

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(p));

					out = new PrintWriter(socket.getOutputStream(), true);
					out.println(msgToSend);
					//Reply from the server
					in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));

					String replyStr = "";
					while(true) {
						replyStr = in.readLine();
						Log.v("Q3", ""+replyStr);
						if(replyStr == null) break;
						if(replyStr.equals("done")) break;

						Log.v("query3", replyStr);

						String[] kv = replyStr.split(",");
						mc.newRow().add("key", kv[0]).add("value", kv[1]);
					}
					if(replyStr.equals("done"))
						return mc;



				} catch (IOException e) {
					e.printStackTrace();
				}
				catch (Exception e) {
					e.printStackTrace();
				}


			}


		}
		mc.newRow().add("key", "Null aaya").add("value", "Shouldn't insert");
		return mc;
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
