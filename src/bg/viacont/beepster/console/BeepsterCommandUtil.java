package bg.viacont.beepster.console;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.h2.util.IOUtils;

import bg.viacont.beepster.communicator.io.BeepsterLocation;
import bg.viacont.beepster.communicator.io.service.message.BeepsterMediaMessage;
import bg.viacont.beepster.communicator.io.service.message.BeepsterMessage;
import bg.viacont.beepster.communicator.io.service.message.BeepsterTextMessage;

public class BeepsterCommandUtil {

	// private static long lastModifiedFrame = 0;
	public final static byte[] readFile(File file) {
		try {
			FileInputStream imageFile = new FileInputStream(file);
			ByteArrayOutputStream imageOut = new ByteArrayOutputStream();
			IOUtils.copy(imageFile, imageOut);
			return imageOut.toByteArray();
		} catch (IOException e) {
			e.printStackTrace(System.err);
			return null;
		}
	}

	public final static File getPhotoFile(Date date, byte cam) {
		DateFormat format = new SimpleDateFormat("MMM-dd-yy", Locale.US);
		String fmt = format.format(date);
		File path = new File("/data/" + fmt + "/cam" + (cam + 1) + "/frame/");
		File nearestFile = null;
		long minDiff = Long.MAX_VALUE;
		for (File file : path.listFiles()) {
			long diff = Math.abs(file.lastModified() - date.getTime());
			if (diff < minDiff) {
				minDiff = diff;
				nearestFile = file;
			}
		}

		return nearestFile;
	}

	public static BeepsterMessage getTextMessage(int toUserId, String text) {

		BeepsterTextMessage textMessage = new BeepsterTextMessage(
				BeepsterConsole.INSTANCE.getDatabase().getUserId(), toUserId,
				BeepsterMessage.MESSAGE_TYPE_TEXT, text);

		return textMessage;
	}

	public static BeepsterMessage getPhotoMessage(int toUserId, byte cam) {

		BeepsterLocation location = BeepsterConsole.INSTANCE.getDatabase()
				.getLastLocation();

		try {
			FileInputStream audioFile = new FileInputStream(
					BeepsterConsole.INSTANCE.getNetwork().getBeepFile());
			ByteArrayOutputStream voiceOut = new ByteArrayOutputStream();
			IOUtils.copy(audioFile, voiceOut);
			BeepsterMediaMessage mediaMessage = new BeepsterMediaMessage(
					BeepsterConsole.INSTANCE.getDatabase().getUserId(),
					toUserId, voiceOut.toByteArray(), getVideoFrame(cam),
					location, null);

			return mediaMessage;
		} catch (IOException e) {
			e.printStackTrace(System.err);
			return null;
		}
	}

	public final static byte[] getVideoFrame(byte camera) {

		try {
			Process process = Runtime
					.getRuntime()
					.exec(MessageFormat
							.format("gst-launch-0.10 v4l2src num-buffers=1 ! image/jpeg,width=320,height=240,framerate=30/1 ! filesink location=/tmp/camera{0}.jpg",
									camera));
			process.waitFor();

			File file = new File(MessageFormat.format("/tmp/camera{0}.jpg",
					camera));

			FileInputStream inputStream = new FileInputStream(file);
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			IOUtils.copy(inputStream, byteOut);
			inputStream.close();
			byteOut.close();

			System.out.println("Captured frame: " + byteOut.size() + " bytes");

			return byteOut.toByteArray();
		} catch (IOException e) {
			e.printStackTrace(System.err);
			return null;
		} catch (InterruptedException e) {
			e.printStackTrace(System.err);
			return null;
		}
	}

	public static void sendSMS(String text, String panicNumber) {

	}

	public static void getSMS(StringBuffer text, StringBuffer number) {
		try {
			Process process = Runtime.getRuntime().exec("gnokii --smsreader ");
			byte[] data = new byte[1024];

			// empty single line in the beginning
			int bytesRead = process.getInputStream().read(data);
			System.out.println(new String(data, 0, bytesRead));

			bytesRead = process.getInputStream().read(data);
			String result = new String(data, 0, bytesRead);
			String[] lines = result.split("\n");
			number.append(lines[0].split("\\: ")[1]);
			String[] texts = lines[1].split("\\: ");
			for (int index = 1; index < texts.length; index++)
				text.append(texts[index]);
			process.destroy();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}

	public static void setLed(String which, boolean on) {
		if (null == which)
			return;

		System.out.println("Setting GPO pin " + which + " to " + on);
		try {
			FileOutputStream fileOut = new FileOutputStream(which);
			fileOut.write((on ? "1" : "0").getBytes());
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private final static void resetGpio(String gpio) {
		setLed(gpio, true);

		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
		}

		setLed(gpio, false);
	}

	public final static void resetModem() {
		setLed(BeepsterConsole.INSTANCE.getNetwork().getModemPowerKeyOut(),
				true);
		try {
			Thread.sleep(1250);
		} catch (InterruptedException e) {
		}
		setLed(BeepsterConsole.INSTANCE.getNetwork().getModemPowerKeyOut(),
				false);

		try {
			Thread.sleep(2500);
		} catch (InterruptedException e) {
		}
		setLed(BeepsterConsole.INSTANCE.getNetwork().getModemResetOut(), false);

		try {
			Thread.sleep(1250);
		} catch (InterruptedException e) {
		}
		setLed(BeepsterConsole.INSTANCE.getNetwork().getModemResetOut(), true);

		try {
			Thread.sleep(750);
		} catch (InterruptedException e) {
		}

		setLed(BeepsterConsole.INSTANCE.getNetwork().getModemPowerKeyOut(),
				true);
		try {
			Thread.sleep(1250);
		} catch (InterruptedException e) {
		}
		setLed(BeepsterConsole.INSTANCE.getNetwork().getModemPowerKeyOut(),
				false);
	}

	public final static void triggerPower() {
		resetGpio(BeepsterConsole.INSTANCE.getNetwork().getModemPowerKeyOut());
	}
}
