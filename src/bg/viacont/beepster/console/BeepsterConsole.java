package bg.viacont.beepster.console;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.jar.Pack200;
import java.util.zip.GZIPInputStream;

import org.h2.util.IOUtils;

import bg.viacont.beepster.communicator.io.BeepsterLocation;
import bg.viacont.beepster.communicator.io.BeepsterUser;
import bg.viacont.beepster.communicator.io.service.message.BeepsterMessage;
import bg.viacont.beepster.communicator.io.service.message.BeepsterStreamMessage;
import bg.viacont.beepster.console.BeepsterModem.BatteryInfo;

public class BeepsterConsole {

	public final static BeepsterConsole INSTANCE = new BeepsterConsole();
	private static String PANIC_SUFFIX = "";
	public final static long MAX_GPS_POLL_TIMEOUT = 8000;
	public final static long POWER_CHECK_PERIOD = 30000;

	private final BeepsterDatabase database = new BeepsterDatabase();
	private final BeepsterNetwork network = new BeepsterNetwork();

	private static boolean wasArmed = false, wasAlarmed = false,
			wasInPanic = false, watchdogEnabled = true;

	private BeepsterLocation lastLocation = null;
	private BeepsterLocation lastLocationMeasured = null;
	private BeepsterLocation currentLocation = null;
	private long lastSynchronize = 0;
	private long lastBatteryCheckStatus = 0;
	// private long restartCounter = System.currentTimeMillis();
	long pollTime = System.currentTimeMillis();
	private boolean isBatteryLow = false;

	private boolean armLevel = false;
	private boolean alarmLevel = false;
	private boolean panicLevel = false;

	protected BeepsterConsole() {
	}

	public final boolean initialize(String rootPath) {
		if (!network.initializeConfig(rootPath))
			return false;

		if (!initDatabase(rootPath))
			return false;

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				database.close();
			}
		});

		BeepsterConsole.INSTANCE.getDatabase().postEvent(
				BeepsterDatabase.EVENT_POWER_ON);
		BeepsterConsole.INSTANCE.getDatabase().postEvent(
				BeepsterDatabase.EVENT_POWER_CONNECTED);

		if (!network.initialize())
			return false;

		// if (network.getGps() != null) {
		// if (isGpsAvailable())
		// BeepsterConsole.INSTANCE.getDatabase().postEvent(
		// BeepsterDatabase.EVENT_GPS_UNAVAILABLE);
		//
		// if (isGpsEnabled())
		// BeepsterConsole.INSTANCE.getDatabase().postEvent(
		// BeepsterDatabase.EVENT_GPS_DISABLED);

		if (network.getGps()) {
			if (!initGps())
				return false;
		}
		// }

		return true;
	}

	private final boolean initGps() {

		System.out.println("Initializing GPS...");

		new Thread() {
			@Override
			public void run() {
				while (true) {
					try {
						if (network.isGpsEnabled() && !isGpsEnabled()) {
							System.out.println("Marking GPS as enabled");
							database.postEvent(BeepsterDatabase.EVENT_GPS_ENABLED);
						}

						if (!network.isGpsEnabled() && isGpsEnabled()) {
							System.out.println("Marking GPS as disabled");
							database.postEvent(BeepsterDatabase.EVENT_GPS_DISABLED);
						}

						if (network.isGpsAvailable() && !isGpsAvailable()) {
							System.out.println("Marking GPS as available");
							database.postEvent(BeepsterDatabase.EVENT_GPS_AVAILABLE);
						}

						if (!network.isGpsAvailable() && isGpsAvailable()) {
							System.out.println("Marking GPS as unavailable");
							database.postEvent(BeepsterDatabase.EVENT_GPS_UNAVAILABLE);
						}

						currentLocation = network.getLocation();

						if (currentLocation.getLatitude() != 0.0
								&& currentLocation.getLongitude() != 0.0) {

							double distance = Double.MAX_VALUE;
							if (null != lastLocation) {
								distance = getDistance(
										lastLocation.getLongitude(),
										currentLocation.getLongitude(),
										lastLocation.getLatitude(),
										currentLocation.getLatitude());

								System.out.println("============");
								System.out.println("Current location: "
										+ currentLocation.getLatitude() + ", "
										+ currentLocation.getLongitude());
								System.out.println("Last location: "
										+ lastLocation.getLatitude() + ", "
										+ lastLocation.getLongitude());
								System.out.println("Distance: " + distance);
							}
							if (distance > network.getMinimumDistance()
									|| (null != lastLocation
											&& lastLocation.getSpeed() >= network
													.getMinimumSpeed() && currentLocation
											.getSpeed() < network
											.getMinimumSpeed())) {

								System.out
										.println("---Storing location in database---");
								database.addLocation(currentLocation);

								lastLocation = currentLocation;
							}

							if (null != lastLocationMeasured) {
								distance = getDistance(
										lastLocationMeasured.getLongitude(),
										currentLocation.getLongitude(),
										lastLocationMeasured.getLatitude(),
										currentLocation.getLatitude());
								if (distance > network
										.getForceSynchronizeDistance())
									lastSynchronize = 0;
							}
						}

						Thread.sleep(1000);
					} catch (Exception e) {
						e.printStackTrace(System.err);
					}
				}
			}
		}.start();

		System.out.println("GPS initialization done");
		return true;
	}

	public static double getDistance(double x1, double x2, double y1, double y2) {
		x1 = x1 * (Math.PI / 180);
		x2 = x2 * (Math.PI / 180);
		y1 = y1 * (Math.PI / 180);
		y2 = y2 * (Math.PI / 180);
		final double dlong = x1 - x2;
		final double dlat = y1 - y2;
		final double a = Math.pow(Math.sin(dlat / 2), 2)
				+ (Math.cos(y1) * Math.cos(y2) * Math.pow(Math.sin(dlong / 2),
						2));
		final double c = 2 * Math.asin(Math.sqrt(a));
		return 6371 * c;
	}

	private final boolean initDatabase(String rootPath) {
		System.out.println("Initializing database...");
		boolean rval = database.load(rootPath);
		System.out.println("Database initialization "
				+ (rval ? "done" : "failed"));

		System.out.println("Compacting...");
		try {
			database.compact(rootPath);
		} catch (SQLException e) {
			System.out.println("Compacting failed");
		} catch (IOException e) {
			System.out.println("Compacting failed");
		}
		System.out.println("Compacting done");

		return rval;
	}

	public final static String getContents(String file) {
		try {
			FileInputStream inputStream = new FileInputStream(file);
			byte[] data = new byte[1024];
			int bytesRead = inputStream.read(data);
			if (bytesRead <= 0) {
				inputStream.close();
				return null;
			}
			inputStream.close();
			return new String(data, 0, bytesRead);
		} catch (IOException e) {
			return null;
		}
	}

	public boolean isGpsEnabled() {
		Map.Entry<Date, Integer> lastDisabled = database
				.getLastEvent(BeepsterDatabase.EVENT_GPS_DISABLED);
		Map.Entry<Date, Integer> lastEnabled = database
				.getLastEvent(BeepsterDatabase.EVENT_GPS_ENABLED);

		if (null == lastDisabled && null == lastEnabled)
			return false;

		if (null != lastDisabled && null == lastEnabled)
			return false;

		if (null == lastDisabled && null != lastEnabled)
			return true;

		return lastEnabled.getKey().getTime() > lastDisabled.getKey().getTime();
	}

	public boolean isGpsAvailable() {
		Map.Entry<Date, Integer> lastDisabled = database
				.getLastEvent(BeepsterDatabase.EVENT_GPS_UNAVAILABLE);
		Map.Entry<Date, Integer> lastEnabled = database
				.getLastEvent(BeepsterDatabase.EVENT_GPS_AVAILABLE);

		if (null == lastDisabled && null == lastEnabled)
			return lastLocation != null;

		if (null != lastDisabled && null == lastEnabled)
			return false;

		if (null == lastDisabled && null != lastEnabled)
			return true;

		return lastEnabled.getKey().getTime() > lastDisabled.getKey().getTime();
	}

	private boolean getPinStatus(String pin) throws FileNotFoundException,
			IOException {

		boolean lastValue = readPinValue(pin);
		for (int index = 0; index < 3; index++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				return lastValue;
			}

			if (readPinValue(pin) != lastValue)
				return false;
		}

		return lastValue;
	}

	private boolean readPinValue(String pin) throws FileNotFoundException,
			IOException {

		byte[] bytes = new byte[128];
		int bytesRead = new FileInputStream(pin).read(bytes);
		return Integer.parseInt(new String(bytes, 0, bytesRead).trim()) == 0 ? false
				: true;
	}

	private final Runnable getPanicButtonRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				// turn off alarm if any
				if (!PANIC_SUFFIX.isEmpty())
					PANIC_SUFFIX = "";

				try {
					boolean status = getPinStatus(network.getPanicIn());

					if (status != panicLevel && !wasInPanic) {
						wasInPanic = true;

						System.out.println("PANIC START");
						PANIC_SUFFIX = " - PANIC";
						// BeepsterCommandUtil.setRedLed(true);

						double lat = 0.0;
						double lon = 0.0;
						BeepsterLocation location = getLastLocation();
						if (null != location) {
							lat = location.getLatitude();
							lon = location.getLongitude();
						}

						if (null != network.getPanicNumber()) {
							BeepsterCommandUtil.sendSMS(MessageFormat.format(
									network.getPanicText(),
									database.getUserName(), lat, lon,
									new Date()), network.getPanicNumber());
						}

						try {
							synchronized (BeepsterConsole.INSTANCE
									.getDatabase()) {
								for (BeepsterUser user : database
										.getPhonebook()) {
									if (user.isOperator()) {
										BeepsterMessage message = BeepsterCommandUtil
												.getTextMessage(user.getId(),
														network.getPanicText());
										if (null != message) {
											message.setDateSent(new Date());
											BeepsterConsole.INSTANCE
													.getDatabase().addMessage(
															message);
										}

										// message = BeepsterCommandUtil
										// .getPhotoMessage(
										// user.getId(),
										// network.getPanicCamera());
										// if (null != message) {
										// BeepsterConsole.INSTANCE
										// .getDatabase().addMessage(
										// message);
										// System.out.println("Message added");
										// }
									}
								}
							}

							System.out.println("SHOULD NOW SYNCHRONIZE!!!");
							lastSynchronize = 0;
						} catch (SQLException e) {
							e.printStackTrace();
						}

						System.out.println("PANIC END");
					} else {
						if (status == panicLevel && wasInPanic) {
							wasInPanic = false;
							System.out.println("PANIC CLEARED");
						}
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace(System.err);
				} catch (IOException e) {
					e.printStackTrace(System.err);
				} catch (NumberFormatException e) {
					e.printStackTrace(System.err);
				}
			}
		};
	}

	private final Runnable getArmedRunnable() {
		return new Runnable() {
			@Override
			public void run() {

				try {
					if (getPinStatus(network.getArmedIn()) == (wasArmed ? armLevel
							: !armLevel)) {

						System.out.println("ARMED START");

						wasArmed = !wasArmed;

						System.out.println("ARMED: " + wasArmed);

						try {
							synchronized (BeepsterConsole.INSTANCE
									.getDatabase()) {
								for (BeepsterUser user : database
										.getPhonebook()) {
									if (user.isOperator()) {
										BeepsterMessage message = BeepsterCommandUtil
												.getTextMessage(
														user.getId(),
														wasArmed ? network
																.getArmedText()
																: network
																		.getDisarmedText());
										if (null != message) {
											message.setDateSent(new Date());
											BeepsterConsole.INSTANCE
													.getDatabase().addMessage(
															message);
											System.out.println("Message added");

										}
									}
								}
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}

						System.out.println("ARMED END");

						System.out.println("WILL NOW SYNCHRONIZE!!!");
						lastSynchronize = 0;
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace(System.err);
				} catch (IOException e) {
					e.printStackTrace(System.err);
				} catch (NumberFormatException e) {
					e.printStackTrace(System.err);
				}
			}
		};
	}

	private final Runnable getAlarmedRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				try {
					if (getPinStatus(network.getAlarmedIn()) == (wasAlarmed ? alarmLevel
							: !alarmLevel)) {

						System.out.println("ALARMED START");

						wasAlarmed = !wasAlarmed;

						System.out.println("ALARMED: " + wasAlarmed);

						try {
							synchronized (BeepsterConsole.INSTANCE
									.getDatabase()) {
								for (BeepsterUser user : database
										.getPhonebook()) {
									if (user.isOperator()) {
										BeepsterMessage message = BeepsterCommandUtil
												.getTextMessage(
														user.getId(),
														wasAlarmed ? network
																.getAlarmedText()
																: network
																		.getDisalarmedText());
										if (null != message) {
											message.setDateSent(new Date());
											BeepsterConsole.INSTANCE
													.getDatabase().addMessage(
															message);
											System.out.println("Message added");
										}
									}
								}
							}
						} catch (SQLException e) {
							e.printStackTrace(System.err);
						}

						System.out.println("ALARMED END");

						System.out.println("WILL NOW SYNCHRONIZE!!!");
						lastSynchronize = 0;
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace(System.err);
				} catch (IOException e) {
					e.printStackTrace(System.err);
				} catch (NumberFormatException e) {
					e.printStackTrace(System.err);
				}
			}
		};
	}

	public final int getConnectTimeout() {
		return database.getConnectTimeout() > 0 ? database.getConnectTimeout()
				: network.getDefaultConnectTimeout();
	}

	public BeepsterDatabase getDatabase() {
		return database;
	}

	public BeepsterNetwork getNetwork() {
		return network;
	}

	public static boolean restart() {
		BeepsterCommandUtil.resetModem();
		System.exit(0);
		return true;
	}

	public static boolean reboot() {
		try {
			Runtime.getRuntime().exec("reboot -f");
			System.exit(-1);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	public final static void main(String[] args) {

		if (0 == args.length) {
			System.out.println("Not enough arguments");
			return;
		}

		BeepsterConsole.INSTANCE.pollTime = System.currentTimeMillis();
		new Thread() {
			@Override
			public void run() {
				while (watchdogEnabled) {
					if ((System.currentTimeMillis() - BeepsterConsole.INSTANCE.pollTime) > 120 * 1000) {
						System.out.println("WATCHDOG: EXIT");
						restart();
					}

					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
					}
				}
			}
		}.start();

		if (!BeepsterConsole.INSTANCE.initialize(args[0])) {
			if (null != BeepsterConsole.INSTANCE.getDatabase())
				BeepsterConsole.INSTANCE.getDatabase().close();
			restart();
		}

		System.out.println("Checking for update...");
		// BeepsterConsole.INSTANCE.checkForUpdate();
		System.out.println("Checking for update done");

		try {
			BeepsterConsole.INSTANCE.armLevel = BeepsterConsole.INSTANCE
					.getPinStatus(BeepsterConsole.INSTANCE.getNetwork()
							.getArmedIn());
			System.out.println("Arm level: "
					+ BeepsterConsole.INSTANCE.armLevel);

			BeepsterConsole.INSTANCE.alarmLevel = BeepsterConsole.INSTANCE
					.getPinStatus(BeepsterConsole.INSTANCE.getNetwork()
							.getAlarmedIn());
			System.out.println("Alarm level: "
					+ BeepsterConsole.INSTANCE.alarmLevel);

			BeepsterConsole.INSTANCE.panicLevel = BeepsterConsole.INSTANCE
					.getPinStatus(BeepsterConsole.INSTANCE.getNetwork()
							.getPanicIn());
			System.out.println("Panic level: "
					+ BeepsterConsole.INSTANCE.panicLevel);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			return;
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}

		new Thread() {
			@Override
			public void run() {
				while (true) {
					BeepsterConsole.INSTANCE.getPanicButtonRunnable().run();
					BeepsterConsole.INSTANCE.getAlarmedRunnable().run();
					BeepsterConsole.INSTANCE.getArmedRunnable().run();
				}
			}
		}.start();

		try {
			BeepsterConsole.INSTANCE.getNetwork().synchronizeClock();
		} catch (IOException e) {
			System.err
					.println("Could not synchronize clock: " + e.getMessage());
		}

		while (true) {
			if (System.currentTimeMillis()
					- BeepsterConsole.INSTANCE.lastSynchronize > BeepsterConsole.INSTANCE
					.getConnectTimeout()) {
				try {
					System.out.println("Synchronizing...");
					BeepsterConsole.INSTANCE.getNetwork().synchronize();
					System.out.println("Synchronization done");
					BeepsterConsole.INSTANCE.lastSynchronize = System
							.currentTimeMillis();

					if (BeepsterConsole.INSTANCE.getNetwork().getGps()) {
						BeepsterConsole.INSTANCE.lastLocationMeasured = BeepsterConsole.INSTANCE
								.getNetwork().getLocation();
					}
				} catch (IOException e) {
					System.err.println("Could not synchronize: "
							+ e.getMessage());
				}
			}

			try {
				float powerLevel = BeepsterConsole.INSTANCE.getNetwork()
						.getPowerLevel();

				System.out.println("Power level: " + powerLevel);
				if (powerLevel < 5.0f) { // 5.0
					watchdogEnabled = false;

					System.out
							.println("No power, shutting down the database...");
					if (BeepsterConsole.INSTANCE.getNetwork().getGps()) {
						BeepsterConsole.INSTANCE.database
								.postEvent(BeepsterDatabase.EVENT_GPS_UNAVAILABLE);
						BeepsterConsole.INSTANCE.database
								.postEvent(BeepsterDatabase.EVENT_GPS_DISABLED);
					}
					BeepsterConsole.INSTANCE.getDatabase().postEvent(
							BeepsterDatabase.EVENT_POWER_DISCONNECTED);
					BeepsterConsole.INSTANCE.database
							.postEvent(BeepsterDatabase.EVENT_SHUTDOWN);

					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
					}

					System.out.println("No power, synchronizing...");
					try {
						BeepsterConsole.INSTANCE.getNetwork().synchronize();
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println("Synchronization done");

					try {
						BeepsterConsole.INSTANCE.database.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println("Database shutdown complete");

					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
					}

					System.out
							.println("Power too low, switching OFF (power key)");
					BeepsterCommandUtil.setLed(BeepsterConsole.INSTANCE
							.getNetwork().getModemPowerKeyOut(), true);

					try {
						Thread.sleep(1250);
					} catch (InterruptedException e) {
					}

					BeepsterCommandUtil.setLed(BeepsterConsole.INSTANCE
							.getNetwork().getModemPowerKeyOut(), false);

					try {
						Thread.sleep(2500);
					} catch (InterruptedException e) {
					}

					System.out
							.println("Power too low, switching OFF (main power)");
					BeepsterCommandUtil.setLed(BeepsterConsole.INSTANCE
							.getNetwork().getPowerOffPin(), false);

					System.exit(0);
				}

				if (System.currentTimeMillis()
						- BeepsterConsole.INSTANCE.lastBatteryCheckStatus > POWER_CHECK_PERIOD) {
					BeepsterConsole.INSTANCE.lastBatteryCheckStatus = System
							.currentTimeMillis();
					BatteryInfo battery = BeepsterConsole.INSTANCE.network
							.getBatteryInfo();
					System.out.println("Battery: " + battery.getBatteryLevel()
							+ "%, " + (battery.getVoltage()) + "V");
					BeepsterConsole.INSTANCE.database.addStreamValue(
							BeepsterStreamMessage.TYPE_SUPPLY_VOLTAGE,
							(int) powerLevel, System.currentTimeMillis());
					BeepsterConsole.INSTANCE.database.addStreamValue(
							BeepsterStreamMessage.TYPE_BATTERY_VOLTAGE,
							(int) battery.getVoltage(),
							System.currentTimeMillis());
					BeepsterConsole.INSTANCE.database
							.addStreamValue(
									BeepsterStreamMessage.TYPE_BATTERY_CHARGE_PERCENTAGE,
									(int) battery.getBatteryLevel(),
									System.currentTimeMillis());
				}

				if (!BeepsterConsole.INSTANCE.isBatteryLow) {
					if (powerLevel < 8) {
						BeepsterConsole.INSTANCE.isBatteryLow = true;
						BeepsterConsole.INSTANCE.getDatabase().postEvent(
								BeepsterDatabase.EVENT_BATTERY_LOW);
					}
				} else {
					if (powerLevel > 9)
						BeepsterConsole.INSTANCE.isBatteryLow = false;
				}

			} catch (IOException e) {
			}

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}

	private void checkForUpdate() {
		try {
			byte[] firmware = BeepsterConsole.INSTANCE.getNetwork()
					.downloadFirmware();
			File file = new File("/beepster/bconsole.jar");
			FileInputStream fileInput = new FileInputStream(file);
			byte[] data = new byte[(int) file.length()];
			fileInput.read(data);
			fileInput.close();
			byte[] md5File = MessageDigest.getInstance("MD5").digest(data);

			File tmpFile = new File("/beepster/bconsole.jar.tmp");
			System.out.println("Checking if update is new");

			JarOutputStream fileOut = new JarOutputStream(new FileOutputStream(
					tmpFile));
			Pack200.Unpacker unpacker = Pack200.newUnpacker();
			unpacker.unpack(new GZIPInputStream(new ByteArrayInputStream(
					firmware)), fileOut);
			fileOut.close();

			data = new byte[(int) tmpFile.length()];
			FileInputStream fileIn = new FileInputStream(tmpFile);
			fileIn.read(data);
			fileIn.close();
			byte[] md5Update = MessageDigest.getInstance("MD5").digest(data);

			if (!Arrays.equals(md5Update, md5File)) {
				System.out.println("There is a new update, shutting down...");
				BeepsterCommandUtil.resetModem();
				database.close();

				System.out.println("Installing new update...");
				FileOutputStream finalOut = new FileOutputStream(file);
				fileIn = new FileInputStream(tmpFile);
				IOUtils.copy(fileIn, finalOut);
				fileIn.close();
				finalOut.close();

				System.out.println("Update installation done. Restarting...");
				System.exit(0);
			} else
				System.out.println("Update is not new, no upgrade");
		} catch (IOException e) {
			System.err.println("Upgrade failed: " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			System.err.println("Upgrade failed: " + e.getMessage());
		}
	}

	public BeepsterLocation getLastLocation() {
		return currentLocation;
	}
}
