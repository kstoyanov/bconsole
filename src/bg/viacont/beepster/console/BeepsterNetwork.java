package bg.viacont.beepster.console;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import bg.viacont.beepster.communicator.io.BeepsterDeviceKeyProvider;
import bg.viacont.beepster.communicator.io.BeepsterLocation;
import bg.viacont.beepster.communicator.io.BeepsterStaticDeviceKeyProvider;
import bg.viacont.beepster.communicator.io.util.BeepsterIOUtil;
import bg.viacont.beepster.communicator.io.util.BeepsterUuidUtil;
import bg.viacont.beepster.console.BeepsterModem.BatteryInfo;
import bg.viacont.beepster.console.resources.BeepsterResources;

public class BeepsterNetwork {

	public final static String BEEPSTER_CONFIG_PATH = "beepster.config";

	public final static String BEEPSTER_SERVER = "BEEPSTER_SERVER";
	public final static String BEEPSTER_DEVICE_ID = "BEEPSTER_DEVICE_ID";
	public final static String BEEPSTER_AUTH_CODE = "BEEPSTER_AUTH_CODE";
	public final static String BEEPSTER_USER_ID = "BEEPSTER_USER_ID";
	public final static String BEEPSTER_BEEP_FILE = "BEEPSTER_BEEP_FILE";
	public final static String BEEPSTER_DEFAULT_CONNECT_TIMEOUT = "BEEPSTER_DEFAULT_CONNECT_TIMEOUT";
	public final static String BEEPSTER_MODEM_RESET_TIMEOUT = "BEEPSTER_MODEM_RESET_TIMEOUT";
	public final static String BEEPSTER_POWER_OFF_PIN = "BEEPSTER_POWER_OFF_PIN";
	public final static String BEEPSTER_MAX_LOCATION_TIME_DIFF = "BEEPSTER_MAX_LOCATION_TIME_DIFF";
	public final static String BEEPSTER_COMMUNICATION_ENCRYPTED = "BEEPSTER_COMMUNICATION_ENCRYPTED";
	public final static String BEEPSTER_LOGIN_KEY = "BEEPSTER_LOGIN_KEY";
	public final static String BEEPSTER_PIN = "BEEPSTER_PIN";
	public final static String BEEPSTER_MODEM_PWRKEY_OUT = "BEEPSTER_MODEM_PWRKEY_OUT";
	public final static String BEEPSTER_MODEM_RESET_OUT = "BEEPSTER_MODEM_RESET_OUT";
	public final static String BEEPSTER_PANIC_IN = "BEEPSTER_PANIC_IN";
	public final static String BEEPSTER_PANIC_TEXT = "BEEPSTER_PANIC_TEXT";
	public final static String BEEPSTER_PANIC_NUMBER = "BEEPSTER_PANIC_NUMBER";
	public final static String BEEPSTER_PANIC_CAMERA = "BEEPSTER_PANIC_CAMERA";
	public final static String BEEPSTER_ARMED_IN = "BEEPSTER_ARMED_IN";
	public final static String BEEPSTER_ARMED_TEXT = "BEEPSTER_ARMED_TEXT";
	public final static String BEEPSTER_DISARMED_TEXT = "BEEPSTER_DISARMED_TEXT";
	public final static String BEEPSTER_ALARMED_IN = "BEEPSTER_ALARMED_IN";
	public final static String BEEPSTER_ALARMED_TEXT = "BEEPSTER_ALARMED_TEXT";
	public final static String BEEPSTER_DISALARMED_TEXT = "BEEPSTER_DISALARMED_TEXT";
	public final static String BEEPSTER_MINIMUM_DISTANCE = "BEEPSTER_MINIMUM_DISTANCE";
	public final static String BEEPSTER_FORCE_SYNCHRONIZE_DISTANCE = "BEEPSTER_FORCE_SYNCHRONIZE_DISTANCE";
	public final static String BEEPSTER_MODEM = "BEEPSTER_MODEM";
	public final static String BEEPSTER_APN = "BEEPSTER_APN";
	public final static String BEEPSTER_GPS = "BEEPSTER_GPS";
	public final static String BEEPSTER_LAN = "BEEPSTER_LAN";
	public final static String BEEPSTER_NO_SIM_SERIAL = "BEEPSTER_NO_SIM_SERIAL";

	public final static String BEEPSTER_VERSION = "1.2.3";
	public final static String BEEPSTER_HEADER_COMMAND = "Command";

	public final static int CONNECTION_TIMEOUT = 8000;
	public final static int DEFAULT_TIMEOUT = 35000;
	public final static int PUSH_TIMEOUT = 32 * 60 * 1000;

	private final static String BEEPSTER_URL_BASE = "http://{0}/beepster?";
	private final static String BEEPSTER_AUTH_URL = BEEPSTER_URL_BASE
			+ "action=auth";
	private final static String BEEPSTER_REFRESH_URL = BEEPSTER_URL_BASE
			+ "action=refresh";
	private final static String BEEPSTER_CLOCK_URL = BEEPSTER_URL_BASE
			+ "action=clock";
	private final static String BEEPSTER_FIRMWARE_URL = "http://{0}/firmware/{1}/bconsole.pack200.gz";

	private Properties config = null;
	private BeepsterStaticDeviceKeyProvider deviceKeyProvider = null;
	private String rootPath = null;
	private BeepsterModem modem = new BeepsterModem();

	public final boolean isRealTimeTrack() {
		return BeepsterConsole.INSTANCE.getDatabase().isRealTimeTrack();
	}

	public int getAuthCode() {
		return Integer
				.parseInt(getConfig().getProperty(BEEPSTER_AUTH_CODE), 16);
	}

	public void setAuthCode(int code) {
		getConfig().setProperty(BEEPSTER_AUTH_CODE, Integer.toString(code, 16));
		saveConfig();
	}

	public boolean getNoSimSerial() {
		return Boolean.parseBoolean(getConfig().getProperty(
				BEEPSTER_NO_SIM_SERIAL));
	}

	public String getSimSerialCode() {
		try {
			if (getNoSimSerial())
				return modem.getImei();
			else
				return modem.getImsi();
		} catch (IOException e) {
			System.err.println("Failed retrieving IMSI number: "
					+ e.getStackTrace());
			return null;
		}
	}

	public String getServer() {
		return getConfig().getProperty(BEEPSTER_SERVER);
	}

	public String getDeviceId() {
		try {
			return modem.getImei();
		} catch (IOException e) {
			System.err.println("Failed retrieving IMEI number: "
					+ e.getStackTrace());
			return null;
		}
	}

	public int getUserId() {
		return BeepsterConsole.INSTANCE.getDatabase().getUserId();
	}

	public String getPin() {
		return getConfig().getProperty(BEEPSTER_PIN, null);
	}

	public String getModemResetOut() {
		return getConfig().getProperty(BEEPSTER_MODEM_RESET_OUT);
	}

	public String getModemPowerKeyOut() {
		return getConfig().getProperty(BEEPSTER_MODEM_PWRKEY_OUT);
	}

	public String getPowerOffPin() {
		return getConfig().getProperty(BEEPSTER_POWER_OFF_PIN);
	}

	public String getPanicIn() {
		return getConfig().getProperty(BEEPSTER_PANIC_IN);
	}

	public String getPanicText() {
		return getConfig().getProperty(BEEPSTER_PANIC_TEXT);
	}

	public String getPanicNumber() {
		return getConfig().getProperty(BEEPSTER_PANIC_NUMBER);
	}

	public String getArmedIn() {
		return getConfig().getProperty(BEEPSTER_ARMED_IN);
	}

	public String getArmedText() {
		return getConfig().getProperty(BEEPSTER_ARMED_TEXT);
	}

	public String getDisarmedText() {
		return getConfig().getProperty(BEEPSTER_DISARMED_TEXT);
	}

	public String getAlarmedIn() {
		return getConfig().getProperty(BEEPSTER_ALARMED_IN);
	}

	public String getAlarmedText() {
		return getConfig().getProperty(BEEPSTER_ALARMED_TEXT);
	}

	public String getDisalarmedText() {
		return getConfig().getProperty(BEEPSTER_DISALARMED_TEXT);
	}

	public String getBeepFile() {
		return getConfig().getProperty(BEEPSTER_BEEP_FILE);
	}

	public byte getPanicCamera() {
		return Byte.parseByte(getConfig().getProperty(BEEPSTER_PANIC_CAMERA));
	}

	public int getDefaultConnectTimeout() {
		return Integer.parseInt(getConfig().getProperty(
				BEEPSTER_DEFAULT_CONNECT_TIMEOUT).trim());
	}

	public long getModemResetTimeout() {
		return Long.parseLong(getConfig().getProperty(
				BEEPSTER_MODEM_RESET_TIMEOUT).trim());
	}

	public int getMaxLocationTimeDiff() {
		return Integer.parseInt(getConfig().getProperty(
				BEEPSTER_MAX_LOCATION_TIME_DIFF));
	}

	public double getMinimumDistance() {
		return Double.parseDouble(getConfig().getProperty(
				BEEPSTER_MINIMUM_DISTANCE));
	}

	public double getForceSynchronizeDistance() {
		return Double.parseDouble(getConfig().getProperty(
				BEEPSTER_FORCE_SYNCHRONIZE_DISTANCE));
	}

	public double getMinimumSpeed() {
		return BeepsterConsole.INSTANCE.getDatabase().getSpeedMargin();
	}

	public String getModem() {
		return getConfig().getProperty(BEEPSTER_MODEM);
	}

	public boolean getGps() {
		return Boolean.parseBoolean(getConfig().getProperty(BEEPSTER_GPS));
	}

	public boolean getLan() {
		return Boolean.parseBoolean(getConfig().getProperty(BEEPSTER_LAN));
	}

	public String getApn() {
		return getConfig().getProperty(BEEPSTER_APN);
	}

	public boolean isEncrypted() {
		return Boolean.parseBoolean(getConfig().getProperty(
				BEEPSTER_COMMUNICATION_ENCRYPTED));
	}

	public PublicKey getLoginPublicKey() {

		try {
			// load the public key from the resource
			byte[] keyBytes = Base64.decodeBase64(getConfig().getProperty(
					BeepsterNetwork.BEEPSTER_LOGIN_KEY));
			X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);

			KeyFactory kf = KeyFactory.getInstance("RSA");
			return kf.generatePublic(spec);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		} catch (InvalidKeySpecException e) {
			throw new RuntimeException(e);
		}
	}

	public final boolean initializeConfig(String rootPath) {
		this.rootPath = rootPath;

		System.out.println("Loading config...");
		config = new Properties();
		try {
			String configPath = MessageFormat.format((String) BeepsterResources
					.getProperties().get(BeepsterNetwork.BEEPSTER_CONFIG_PATH),
					rootPath);
			config.load(new FileInputStream(configPath));
		} catch (IOException e) {
			e.printStackTrace(System.err);
			return false;
		}

		System.out.println("Config loading done");
		return true;
	}

	public final void saveConfig() {
		try {
			String configPath = MessageFormat.format((String) BeepsterResources
					.getProperties().get(BeepsterNetwork.BEEPSTER_CONFIG_PATH),
					rootPath);
			config.store(new FileOutputStream(configPath), null);
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
	}

	public final boolean initialize() {

		System.out.println("Opening modem ...");
		if (!modem.open(getModem()))
			throw new RuntimeException("Could not open modem " + getModem());

		try {
			modem.initialize(getPin(), getGps());
			modem.setOnline(getApn());
		} catch (IOException e) {
			System.err.println("Error initializing modem: " + e.getMessage());

			System.out.println("Closing modem");
			modem.close();
			return false;
		}

		int userId = BeepsterConsole.INSTANCE.getDatabase().getUserId();
		deviceKeyProvider = new BeepsterStaticDeviceKeyProvider(userId,
				BeepsterUuidUtil.getUuidBytes(getDeviceUUID()));

		try {
			if (userId == -1) {
				System.out.println("Authenticating...");
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
				}
				authenticate(deviceKeyProvider.getDeviceKey(userId));
				System.out.println("Authentication done");

				return true;
			}

			return true;
		} catch (IOException e) {
			System.err.println("Error initializing modem: " + e.getMessage());
			return false;
		}
	}

	private final Properties getConfig() {
		return config;
	}

	public final void authenticate(byte[] deviceKey) throws IOException {
		byte[] authData = BeepsterIOUtil.getAuthData(getLoginPublicKey(),
				getAuthCode(), deviceKey, getSimSerialCode());
		String url = MessageFormat.format(BEEPSTER_AUTH_URL, getServer());
		byte[] ret = modem.postHttp(url.toString(), authData, getLan());
		System.out.println("Received: " + (ret != null ? ret.length : 0)
				+ " bytes");
		BeepsterIOUtil.processAuthResponse(new ByteArrayInputStream(ret),
				BeepsterConsole.INSTANCE.getDatabase());
		deviceKeyProvider.setUserId(BeepsterConsole.INSTANCE.getDatabase()
				.getUserId());
	}

	public final Date synchronizeClock() throws IOException {

		System.out.println("Synchronizing clock...");
		String url = MessageFormat.format(BEEPSTER_CLOCK_URL, getServer());
		byte[] serverSyncData = modem.postHttp(url.toString(), null, getLan());
		System.out.println("Received: "
				+ (serverSyncData != null ? serverSyncData.length : 0)
				+ " bytes");
		if (null == serverSyncData || 0 == serverSyncData.length)
			return null;
		long dateValue = Long.parseLong(new String(serverSyncData).trim());
		Date date = new Date(dateValue);
		modem.setClock(date);
		modem.syncClock();
		System.out.println("Clock synchronization done.");
		return date;
	}

	public final String synchronize() throws IOException {

		System.out.println("Packing ECU values...");
		BeepsterConsole.INSTANCE.getDatabase().packECUValues();
		System.out.println("Packing ECU values done");

		int userAuthCode = getAuthCode();
		byte[] clientSyncData = BeepsterIOUtil
				.getClientSyncData(BEEPSTER_VERSION, (byte) 0, null, null, 0,
						0, (short) 0, (short) 0,
						BeepsterConsole.INSTANCE.getDatabase(), userAuthCode,
						deviceKeyProvider.getDeviceKey(getUserId()), true);

		System.out.println("Synchronizing with user auth code: "
				+ Integer.toString(userAuthCode, 16));

		String url = MessageFormat.format(BEEPSTER_REFRESH_URL, getServer());
		byte[] serverSyncData = modem.postHttp(url.toString(), clientSyncData,
				getLan());
		System.out.println("Received: "
				+ (serverSyncData != null ? serverSyncData.length : 0)
				+ " bytes");
		if (null == serverSyncData || 0 == serverSyncData.length)
			return null;
		HashSet<Integer> onlineUsers = new HashSet<Integer>();
		String rval = BeepsterIOUtil.processServerSyncData(serverSyncData,
				BeepsterConsole.INSTANCE.getDatabase(), onlineUsers,
				deviceKeyProvider.getDeviceKey(getUserId()));
		System.out.println("Version: " + rval);
		return rval;
	}

	public UUID getDeviceUUID() {

		String imei = getDeviceId();
		String simSerial = getSimSerialCode();

		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			return null;
		}

		System.out.println("IMEI: " + imei);
		System.out.println("SIM: " + simSerial);
		System.out.println("PIN: " + getPin());

		messageDigest.update(imei.getBytes());
		messageDigest.update(simSerial.getBytes());
		if (getPin() != null)
			messageDigest.update(getPin().getBytes());

		ByteBuffer byteBuffer = ByteBuffer.wrap(messageDigest.digest());
		long most = byteBuffer.getLong();
		long least = byteBuffer.getLong();

		System.out.println("UUID: " + new UUID(most, least).toString());

		return new UUID(most, least);
	}

	public BeepsterDeviceKeyProvider getDeviceKeyProvider() {
		return deviceKeyProvider;
	}

	public String getHardwareSerial() {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream("/proc/cpuinfo")));
			for (String line = reader.readLine(); line != null; line = reader
					.readLine()) {
				String[] parts = line.split("\\:");

				// empty line read
				if (2 != parts.length)
					continue;

				if (parts[0].trim().equals("Serial"))
					return parts[1].trim();

				System.out.println(parts[0].trim() + ":" + parts[1].trim());
			}
		} catch (IOException e) {
			System.err.println("Could not read hardware serial number: "
					+ e.getMessage());
			e.printStackTrace();
		}

		System.err.println("Serial not found.");
		return null;
	}

	public boolean isGpsEnabled() throws IOException {
		return modem.isGpsEnabled();
	}

	public boolean isGpsAvailable() throws IOException {
		return modem.isGpsAvailable();
	}

	public BatteryInfo getBatteryInfo() throws IOException {
		return modem.getBatteryInfo();
	}

	public float getPowerLevel() throws IOException {
		return modem.getPowerLevel();
	}

	public BeepsterLocation getLocation() throws IOException {
		return modem.getLocation();
	}

	public byte[] downloadFirmware() throws IOException {
		String url = MessageFormat.format(BEEPSTER_FIRMWARE_URL, getServer(),
				BeepsterConsole.INSTANCE.getDatabase().getUserId());
		return modem.postHttp(url, null, getLan());
	}
}
