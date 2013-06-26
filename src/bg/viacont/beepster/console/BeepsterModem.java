package bg.viacont.beepster.console;

import gnu.io.CommPort;
import gnu.io.CommPortIdentifier;
import gnu.io.NoSuchPortException;
import gnu.io.PortInUseException;
import gnu.io.SerialPort;
import gnu.io.UnsupportedCommOperationException;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.h2.util.IOUtils;

import bg.viacont.beepster.communicator.io.BeepsterLocation;

public class BeepsterModem {

	private final static String LE = "\r\n";
	private final static String BEEPSTER_CONSOLE_ID = "Beepster Console/1.0";
	private final static String RESPONSE_OK = "OK";

	private final static int MAX_SAPBR_ATTEMPTS = 6;
	private final static int MAX_CGATT_ATTEMPTS = 6;

	private final static int NUM_POWER_LEVEL_SAMPLES = 3;

	private SerialPort serialPort;
	private DataInputStream inputStream;
	private OutputStream outputStream;

	public class BatteryInfo {
		public final static int NOT_CHARGING = 0;
		public final static int CHARGING = 1;
		public final static int CHARGING_FINISHED = 2;

		private int chargeStatus;
		private int batteryLevel;
		private float voltage;

		public BatteryInfo(int chargeStatus, int batteryLevel, float voltage) {
			this.chargeStatus = chargeStatus;
			this.batteryLevel = batteryLevel;
			this.voltage = voltage;
		}

		public int getChargeStatus() {
			return chargeStatus;
		}

		public int getBatteryLevel() {
			return batteryLevel;
		}

		public float getVoltage() {
			return voltage;
		}
	};

	public boolean open(String modem) {
		synchronized (this) {
			System.out.println("Opening modem " + modem);

			try {
				CommPortIdentifier portIdentifier = CommPortIdentifier
						.getPortIdentifier(modem);
				if (portIdentifier.isCurrentlyOwned()) {
					System.out
							.println("Modem initialization failed: com port in use");
					return false;
				}

				CommPort commPort = portIdentifier.open(this.getClass()
						.getName(), 3000);

				if (commPort instanceof SerialPort) {
					serialPort = (SerialPort) commPort;
					serialPort.setSerialPortParams(460800,
							SerialPort.DATABITS_8, SerialPort.STOPBITS_1,
							SerialPort.PARITY_NONE);
					// serialPort.disableReceiveTimeout();
					// serialPort.enableReceiveThreshold(1);

					inputStream = new DataInputStream(
							serialPort.getInputStream());
					outputStream = serialPort.getOutputStream();
				}
			} catch (UnsupportedCommOperationException e) {
				System.out.println("Modem initialization failed: "
						+ e.getMessage());
				e.printStackTrace(System.err);
				return false;
			} catch (NoSuchPortException e) {
				System.out.println("Modem initialization failed: "
						+ e.getMessage());
				e.printStackTrace(System.err);
				return false;
			} catch (PortInUseException e) {
				System.out.println("Modem initialization failed: "
						+ e.getMessage());
				e.printStackTrace(System.err);
				return false;
			} catch (IOException e) {
				System.out.println("Modem initialization failed: "
						+ e.getMessage());
				e.printStackTrace(System.err);
				return false;
			}

			return true;
		}
	}

	public void close() {

		synchronized (this) {
			try {
				inputStream.close();
			} catch (IOException e) {
				System.out.println("Failed closing input stream: "
						+ e.getMessage());
			}

			try {
				outputStream.close();
			} catch (IOException e) {
				System.out.println("Failed closing output stream: "
						+ e.getMessage());
			}

			serialPort.removeEventListener();
			serialPort.close();
		}
	}

	public OutputStream getOutputStream() {
		return outputStream;
	}

	public DataInputStream getInputStream() {
		return inputStream;
	}

	public void initialize(String pin, boolean hasGPS) throws IOException {

		synchronized (this) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}

			writeCommand("ATZ");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed resetting modem");

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}

			writeCommand("AT+CPIN=?");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed preparing PIN");

			if (null != pin) {
				writeCommand("AT+CPIN=" + pin);
				readLine();
				if (!isOK(readLine()))
					throw new IOException("Failed entering PIN");
			}

			if (hasGPS) {
				writeCommand("AT+CGPSPWR=1");
				readLine();
				if (!isOK(readLine()))
					throw new IOException("Failed starting GPS");

				writeCommand("AT+CGPSRST=0");
				readLine();
				if (!isOK(readLine()))
					throw new IOException("Failed resetting GPS");
			}

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}

			writeCommand("AT+CLTS=1");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed setting clock by network");

			syncClock();
		}
	}

	public String getImsi() throws IOException {
		synchronized (this) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}

			writeCommand("AT+CIMI");
			readLine();
			String cimi = readLine();
			readLine();
			return cimi;
		}
	}

	public String getImei() throws IOException {
		synchronized (this) {
			writeCommand("AT+GSN");
			readLine();
			String imei = readLine();
			readLine();
			return imei;
		}
	}

	public void setOnline(String apn) throws IOException {
		synchronized (this) {
			writeCommand("AT+IFC=2,2");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed setting software flow of control");

			int index = 1;
			for (; index < MAX_CGATT_ATTEMPTS; index++) {
				writeCommand("AT+CGATT=1");
				readLine();
				if (isOK(readLine()))
					break;

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}

			if (index == MAX_CGATT_ATTEMPTS + 1)
				throw new IOException("Error attaching to GPRS service");

			writeCommand("AT+SAPBR=3,1,\"CONTYPE\",\"GPRS\"");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed switching in GPRS mode");

			writeCommand("AT+SAPBR=3,1,\"APN\",\"" + apn + "\"");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed setting APN to " + apn);

			int i = 1;
			for (; i <= MAX_SAPBR_ATTEMPTS; i++) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}

				writeCommand("AT+SAPBR=1,1");
				readLine();
				if (isOK(readLine()))
					break;
			}

			if (MAX_SAPBR_ATTEMPTS + 1 == i)
				throw new IOException("Failed opening bearer");
		}

		writeCommand("AT+HTTPINIT");
		readLine();
		if (!isOK(readLine()))
			throw new IOException("Failed setting HTTP mode");

		writeCommand("AT+HTTPPARA=\"CID\",1");
		readLine();
		if (!isOK(readLine()))
			throw new IOException("Failed setting CID to 1");

		writeCommand("AT+HTTPPARA=\"UA\",\"" + BEEPSTER_CONSOLE_ID + "\"");
		readLine();
		if (!isOK(readLine()))
			throw new IOException("Failed setting UA to " + BEEPSTER_CONSOLE_ID);
	}

	public void setOffline() throws IOException {
		writeCommand("AT+HTTPTERM");
		readLine();
		if (!isOK(readLine()))
			throw new IOException("Failed terminating HTTP");

		writeCommand("AT+SAPBR=0,1");
		readLine();
		if (!isOK(readLine()))
			throw new IOException("Failed closing bearer");
	}

	public byte[] postHttp(String url, byte[] data, boolean isLan)
			throws IOException {

		if (isLan) {
			try {
				URL composedUrl = new URL(url);
				HttpURLConnection connection = (HttpURLConnection) composedUrl
						.openConnection();
				connection.setConnectTimeout(3000);
				connection.setReadTimeout(3000);
				connection.setRequestMethod("POST");
				connection.setRequestProperty("Content-Type",
						"application/binary");
				connection.setDoInput(true);
				if (data != null) {
					connection.setRequestProperty("Content-Length", ""
							+ Integer.toString(data.length));
					connection.setDoOutput(true);
					connection.getOutputStream().write(data);
					connection.getOutputStream().close();
				}

				int code = connection.getResponseCode();
				System.out.println("Response code: " + code);

				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				IOUtils.copy(connection.getInputStream(), byteOut);
				byteOut.close();
				System.out.println("Content length: " + byteOut.size());
				System.out.println("Closing connection...");
				connection.getInputStream().close();
				connection.disconnect();
				return byteOut.toByteArray();
			} catch (IOException e) {
				System.err
						.println("Failed sending via LAN - " + e.getMessage());
			}
		}

		synchronized (this) {
			writeCommand("AT+HTTPPARA=\"URL\",\"" + url + "\"");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed setting URL to " + url);

			if (null != data) {
				writeCommand("AT+HTTPDATA=" + data.length + ",2000");
				readLine();
				readLine();

				outputStream.write(data);
				outputStream.flush();

				if (!isOK(readLine()))
					throw new IOException(
							"Failed commencing HTTP POST transfer");
			}

			writeCommand("AT+HTTPACTION=1");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed setting HTTP action to 1");

			byte[] ret = null;
			String[] response = readLine().split(",");
			if (response.length == 3 && Integer.parseInt(response[2]) > 0) {
				writeCommand("AT+HTTPREAD=0," + Integer.parseInt(response[2]));
				readLine();
				readLine();

				ret = new byte[Integer.parseInt(response[2])];
				inputStream.readFully(ret);

				if (!isOK(readLine()))
					throw new IOException("Failed reading data");
			}

			return ret;
		}
	}

	public void setClock(Date date) throws IOException {
		synchronized (this) {
			SimpleDateFormat sdf = new SimpleDateFormat("yy/MM/dd,hh:mm:ssZ",
					Locale.ENGLISH);
			String fmt = sdf.format(date);
			fmt = fmt.substring(0, fmt.length() - 2);
			writeCommand("AT+CCLK=\"" + fmt + "\"");
			readLine();
			if (!isOK(readLine()))
				throw new IOException("Failed setting modem clock");
		}
	}

	public void syncClock() throws IOException {
		synchronized (this) {
			writeCommand("AT+CCLK?");
			readLine();
			String clock = readLine();

			clock = clock.split("\\\"")[1] + "00";
			SimpleDateFormat sdf = new SimpleDateFormat("yy/MM/dd,HH:mm:ssZ",
					Locale.ENGLISH);
			SimpleDateFormat ddf = new SimpleDateFormat(
					"dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);
			try {
				Date date = sdf.parse(clock);
				System.out.println("Parsed date: " + date);
				ProcessBuilder processBuilder = new ProcessBuilder("date",
						"-s", ddf.format(date));
				System.out.println("Executing: " + processBuilder);
				Process process = processBuilder.start();
				process.waitFor();
				processBuilder = new ProcessBuilder("hwclock", "--systohc");
				System.out.println("Executing: " + processBuilder);
				process = processBuilder.start();
				process.waitFor();
			} catch (ParseException e) {
				throw new IOException(e);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}

			if (!isOK(readLine()))
				throw new IOException("Failed retrieving clock");
		}
	}

	public boolean isGpsEnabled() throws IOException {
		synchronized (this) {
			writeCommand("AT+CGPSPWR?");
			readLine();
			boolean isEnabled = Integer.parseInt(readLine().split(":")[1]
					.trim()) > 0 ? true : false;
			if (!isOK(readLine()))
				throw new IOException("Failed reading GPS enabled status");
			return isEnabled;
		}
	}

	public boolean isGpsAvailable() throws IOException {
		synchronized (this) {
			writeCommand("AT+CGPSSTATUS?");
			readLine();
			String fix = readLine().split(":")[1].trim();
			boolean isAvailable = fix.equals("Location 2D Fix")
					|| fix.equals("Location 3D Fix");
			if (!isOK(readLine()))
				throw new IOException("Failed reading GPS availability status");
			return isAvailable;
		}
	}

	public BatteryInfo getBatteryInfo() throws IOException {
		synchronized (this) {
			writeCommand("AT+CBC");
			readLine();
			String[] entries = readLine().split(":")[1].trim().split(",");
			if (!isOK(readLine()))
				throw new IOException("Failed reading battery status");
			return new BatteryInfo(Integer.parseInt(entries[0]),
					Integer.parseInt(entries[1]),
					Float.parseFloat(entries[2]) / 1000.0f);
		}
	}

	private float doGetPowerLevel() throws IOException {
		writeCommand("AT+CADC?");
		readLine();
		String level = readLine().split(":")[1].split(",")[1].trim();
		if (!isOK(readLine()))
			throw new IOException("Failed reading power level");
		return Float.parseFloat(level) * 1.15f / 100.0f;
	}

	public float getPowerLevel() throws IOException {
		synchronized (this) {
			float powerLevel = doGetPowerLevel();
			for (int index = 0; index < NUM_POWER_LEVEL_SAMPLES - 1; index++) {
				powerLevel += doGetPowerLevel();
				powerLevel /= 2;
			}

			return powerLevel;
		}
	}

	public BeepsterLocation getLocation() throws IOException {

		synchronized (this) {
			writeCommand("AT+CGPSINF=0");
			readLine();
			String data[] = readLine().split(",");
			double longitude = Double.parseDouble(data[1].substring(0, 2))
					+ Double.parseDouble(data[1].substring(2)) / 60.0;
			double latitude = Double.parseDouble(data[2].substring(0, 2))
					+ Double.parseDouble(data[2].substring(2)) / 60.0;
			double altitude = Double.parseDouble(data[3]);
			Date date;
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyyMMddHHmmss.SSS");
				sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
				date = sdf.parse(data[4]);
			} catch (ParseException e) {
				throw new IOException(e);
			}

			System.out.println("Date: " + date);
			Double speed = Double.parseDouble(data[7]);

			if (!isOK(readLine()))
				throw new IOException("Failed reading location");

			return new BeepsterLocation(date, latitude, longitude, altitude,
					speed, true);
		}
	}

	public final void writeCommand(String line) throws IOException {
		System.out.println("WriteLine: " + line);
		outputStream.write((line + LE).getBytes());
		outputStream.flush();

		// try {
		// Thread.sleep(250);
		// } catch (InterruptedException e) {
		// }
	}

	private final String readLine() throws IOException {

		byte[] c = new byte[1];
		String rval = "";
		while (!rval.endsWith(LE)) {
			inputStream.readFully(c);
			rval += new String(c);
		}

		rval = rval.trim();

		BeepsterConsole.INSTANCE.pollTime = System.currentTimeMillis();

		if (rval.isEmpty())
			return readLine();
		else {
			System.out.println("ReadLine: " + rval);
			return rval;
		}
	}

	private final static boolean isOK(String response) {
		return RESPONSE_OK.equals(response);
	}
}
