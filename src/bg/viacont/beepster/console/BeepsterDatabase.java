package bg.viacont.beepster.console;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import bg.viacont.beepster.communicator.io.BeepsterLocation;
import bg.viacont.beepster.communicator.io.BeepsterUser;
import bg.viacont.beepster.communicator.io.service.BeepsterDatabaseClientService;
import bg.viacont.beepster.communicator.io.service.message.BeepsterMessage;
import bg.viacont.beepster.communicator.io.service.message.BeepsterStreamMessage;
import bg.viacont.beepster.communicator.io.service.message.BeepsterStreamMessage.SubStream;
import bg.viacont.beepster.console.resources.BeepsterResources;

public class BeepsterDatabase implements BeepsterDatabaseClientService {

	private final static String SQL_ENCODING = "UTF-8";

	private final static String SQL_GET_MESSAGE_BY_INTERNAL_ID = "SQL_GET_MESSAGE_BY_INTERNAL_ID";
	private final static String SQL_GET_MESSAGE_BY_ID = "SQL_GET_MESSAGE_BY_ID";
	private final static String SQL_DELETE_SYNCHRONIZED_MESSAGES = "SQL_DELETE_SYNCHRONIZED_MESSAGES";
	private final static String SQL_ADD_MESSAGE = "SQL_ADD_MESSAGE";
	private final static String SQL_UPDATE_MESSAGE_STATUS = "SQL_UPDATE_MESSAGE_STATUS";
	private final static String SQL_UPDATE_MESSAGE_ID = "SQL_UPDATE_MESSAGE_ID";
	private final static String SQL_DELETE_MESSAGE = "SQL_DELETE_MESSAGE";
	private final static String SQL_ADD_LOCATION = "SQL_ADD_LOCATION";
	private final static String SQL_GET_GPS_LOCATIONS = "SQL_GET_GPS_LOCATIONS";
	private final static String SQL_GET_LAST_LOCATION = "SQL_GET_LAST_LOCATION";
	private final static String SQL_CLEAR_LOCATIONS = "SQL_CLEAR_LOCATIONS";
	private final static String SQL_DELETE_LOCATION = "SQL_DELETE_LOCATION";
	private final static String SQL_POST_EVENT = "SQL_POST_EVENT";
	private final static String SQL_GET_EVENTS = "SQL_GET_EVENTS";
	private final static String SQL_GET_LAST_EVENT = "SQL_GET_LAST_EVENT";
	private final static String SQL_CLEAR_EVENTS = "SQL_CLEAR_EVENTS";
	private final static String SQL_ADD_STREAM_VALUE = "SQL_ADD_STREAM_VALUE";
	private final static String SQL_GET_STREAM_VALUES = "SQL_GET_STREAM_VALUES";
	private final static String SQL_GET_LAST_STREAM_VALUE = "SQL_GET_LAST_STREAM_VALUE";
	private final static String SQL_DELETE_STREAM_VALUES = "SQL_DELETE_STREAM_VALUES";
	private final static String SQL_GET_PHONEBOOK = "SQL_GET_PHONEBOOK";
	private final static String SQL_CLEAR_PHONEBOOK = "SQL_CLEAR_PHONEBOOK";
	private final static String SQL_ADD_PHONEBOOK_ENTRY = "SQL_ADD_PHONEBOOK_ENTRY";
	private final static String SQL_GET_INBOX_MESSAGES = "SQL_GET_INBOX_MESSAGES";
	private final static String SQL_GET_OUTBOX_MESSAGES = "SQL_GET_OUTBOX_MESSAGES";
	private final static String SQL_GET_USER_BY_ID = "SQL_GET_USER_BY_ID";
	private final static String SQL_SET_USER_PHONES = "SQL_SET_USER_PHONES";
	private final static String SQL_GET_USER_PHONES = "SQL_GET_USER_PHONES";
	private final static String SQL_CLEAR_OVERDUE_MESSAGES = "SQL_CLEAR_OVERDUE_MESSAGES";
	private final static String SQL_DELETE_STREAM_MESSAGES = "SQL_DELETE_STREAM_MESSAGES";
	private final static String SQL_GET_USER_ID = "SQL_GET_USER_ID";
	private final static String SQL_SET_USER_ID = "SQL_SET_USER_ID";
	private final static String SQL_GET_USER_OPERATOR = "SQL_GET_USER_OPERATOR";
	private final static String SQL_SET_USER_OPERATOR = "SQL_SET_USER_OPERATOR";
	private final static String SQL_GET_IS_REAL_TIME_TRACK = "SQL_GET_IS_REAL_TIME_TRACK";
	private final static String SQL_SET_IS_REAL_TIME_TRACK = "SQL_SET_IS_REAL_TIME_TRACK";
	private final static String SQL_GET_USER_NAME = "SQL_GET_USER_NAME";
	private final static String SQL_SET_USER_NAME = "SQL_SET_USER_NAME";
	private final static String SQL_GET_USER_EMAIL = "SQL_GET_USER_EMAIL";
	private final static String SQL_SET_USER_EMAIL = "SQL_SET_USER_EMAIL";
	private final static String SQL_GET_SPEED_MARGIN = "SQL_GET_SPEED_MARGIN";
	private final static String SQL_SET_SPEED_MARGIN = "SQL_SET_SPEED_MARGIN";
	private final static String SQL_GET_WAIT_MARGIN = "SQL_GET_WAIT_MARGIN";
	private final static String SQL_SET_WAIT_MARGIN = "SQL_SET_WAIT_MARGIN";
	private final static String SQL_GET_CONNECT_TIMEOUT = "SQL_GET_CONNECT_TIMEOUT";
	private final static String SQL_SET_CONNECT_TIMEOUT = "SQL_SET_CONNECT_TIMEOUT";
	private final static String SQL_GET_INBOUND_BYTES_LIMIT = "SQL_GET_INBOUND_BYTES_LIMIT";
	private final static String SQL_SET_INBOUND_BYTES_LIMIT = "SQL_SET_INBOUND_BYTES_LIMIT";
	private final static String SQL_GET_OUTBOUND_BYTES_LIMIT = "SQL_GET_OUTBOUND_BYTES_LIMIT";
	private final static String SQL_SET_OUTBOUND_BYTES_LIMIT = "SQL_SET_OUTBOUND_BYTES_LIMIT";
	private final static String SQL_GET_INBOUND_BYTES = "SQL_GET_INBOUND_BYTES";
	private final static String SQL_SET_INBOUND_BYTES = "SQL_SET_INBOUND_BYTES";
	private final static String SQL_GET_OUTBOUND_BYTES = "SQL_GET_OUTBOUND_BYTES";
	private final static String SQL_SET_OUTBOUND_BYTES = "SQL_SET_OUTBOUND_BYTES";

	private final static String BEEPSTER_JDBC_DRIVER = "beepster.database.jdbc.driver";
	private final static String BEEPSTER_JDBC_CONNECTION = "beepster.database.jdbc.connection";
	private final static String BEEPSTER_JDBC_USER = "beepster.database.jdbc.user";
	private final static String BEEPSTER_JDBC_PASS = "beepster.database.jdbc.pass";
	private final static String BEEPSTER_JDBC_SCHEMA_CREATE = "beepster.database.jdbc.schema.create";
	private final static String BEEPSTER_JDBC_SCHEMA_UPGRADE = "beepster.database.jdbc.schema.upgrade";
	private final static String BEEPSTER_JDBC_SCHEMA_COMPACT = "beepster.database.jdbc.schema.compact";
	private final static String BEEPSTER_JDBC_SCHEMA_DROP = "beepster.database.jdbc.schema.drop";
	private final static String BEEPSTER_JDBC_DB_LOCATION = "beepster.database.jdbc.db.location";

	public static final int EVENT_GPS_DISABLED = 0;
	public static final int EVENT_GPS_ENABLED = 1;
	public static final int EVENT_GPS_AVAILABLE = 2;
	public static final int EVENT_GPS_UNAVAILABLE = 3;
	public static final int EVENT_DATA_CONNECTED = 4;
	public static final int EVENT_DATA_DISCONNECTED = 5;
	public static final int EVENT_POWER_ON = 6;
	public static final int EVENT_SHUTDOWN = 7;
	public static final int EVENT_SCREEN_ON = 11;
	public static final int EVENT_SCREEN_OFF = 12;
	public static final int EVENT_ECU_CONNECTED = 18;
	public static final int EVENT_ECU_DISCONNECTED = 19;
	public static final int EVENT_POWER_CONNECTED = 8;
	public static final int EVENT_POWER_DISCONNECTED = 9;
	public static final int EVENT_BATTERY_LOW = 10;

	private LinkedList<PreparedStatement> statements = new LinkedList<PreparedStatement>();

	private Connection connection = null;

	private final String loadSQL(String id) throws IOException {
		// load the schema name from the properties
		String jdbcSchema = BeepsterResources.getProperties().getProperty(id);

		// load the schema SQL text from the resources
		InputStream inputStream = BeepsterResources.class
				.getResourceAsStream(jdbcSchema);
		byte[] bytes = new byte[inputStream.available()];
		inputStream.read(bytes);
		String sql = new String(bytes, SQL_ENCODING);
		return sql;
	}

	private final String loadStatement(String id) throws IOException {
		return BeepsterResources.getProperties().getProperty(id);
	}

	public final boolean load(String rootPath) {

		try {
			// load JDBC initialization strings
			String jdbcDriver = BeepsterResources.getProperties().getProperty(
					BEEPSTER_JDBC_DRIVER);
			String jdbcConnection = MessageFormat.format(BeepsterResources
					.getProperties().getProperty(BEEPSTER_JDBC_CONNECTION),
					rootPath);
			String jdbcUser = BeepsterResources.getProperties().getProperty(
					BEEPSTER_JDBC_USER);
			String jdbcPass = BeepsterResources.getProperties().getProperty(
					BEEPSTER_JDBC_PASS);

			// make sure the JDBC driver exists
			Class.forName(jdbcDriver);

			// check if the DB exists
			String dbLocation = BeepsterResources.getProperties().getProperty(
					BEEPSTER_JDBC_DB_LOCATION);
			String dbFile = MessageFormat.format(dbLocation, rootPath);
			boolean shouldInitialize = !new File(dbFile).exists();

			// initialize the database connection
			connection = DriverManager.getConnection(jdbcConnection, jdbcUser,
					jdbcPass);

			if (shouldInitialize)
				createSchema();

			upgrade();
		} catch (SQLException e) {
			e.printStackTrace(System.err);
			return false;
		} catch (IOException e) {
			e.printStackTrace(System.err);
			return false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace(System.err);
			return false;
		}

		return true;
	}

	public final void close() {
		try {
			for (PreparedStatement statement : statements)
				statement.close();

			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private final void createSchema() throws SQLException, IOException {
		PreparedStatement sqlInitializeDatabase = connection
				.prepareStatement(loadSQL(BEEPSTER_JDBC_SCHEMA_CREATE));
		try {
			sqlInitializeDatabase.execute();
		} finally {
			sqlInitializeDatabase.close();
		}
	}

	public final void dropSchema() throws SQLException, IOException {
		PreparedStatement sqlDropDatabase = connection
				.prepareStatement(loadSQL(BEEPSTER_JDBC_SCHEMA_DROP));
		try {
			sqlDropDatabase.execute();
		} finally {
			sqlDropDatabase.close();
		}
	}

	public final void upgrade() throws SQLException, IOException {
		PreparedStatement sqlUpgradeDatabase = connection
				.prepareStatement(loadSQL(BEEPSTER_JDBC_SCHEMA_UPGRADE));
		try {
			sqlUpgradeDatabase.execute();
		} finally {
			sqlUpgradeDatabase.close();
		}
	}

	public final void compact(String rootPath) throws SQLException, IOException {
		PreparedStatement sqlCompactDatabase = connection
				.prepareStatement(loadSQL(BEEPSTER_JDBC_SCHEMA_COMPACT));
		try {
			sqlCompactDatabase.execute();
		} finally {
			sqlCompactDatabase.close();
		}
		close();

		load(rootPath);
	}

	public final void addLocation(BeepsterLocation BeepsterLocation) {

		try {
			PreparedStatement sqlAddLocation = connection
					.prepareStatement(loadStatement(SQL_ADD_LOCATION));
			try {
				sqlAddLocation.setDouble(1, BeepsterLocation.getLatitude());
				sqlAddLocation.setDouble(2, BeepsterLocation.getLongitude());
				sqlAddLocation.setDouble(3, BeepsterLocation.getAltitude());
				sqlAddLocation.setDouble(4, BeepsterLocation.getSpeed());
				sqlAddLocation.setTimestamp(5, new Timestamp(BeepsterLocation
						.getDate().getTime()));
				sqlAddLocation.execute();
			} finally {
				sqlAddLocation.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final List<BeepsterLocation> getGPSLocations() {
		try {
			LinkedList<BeepsterLocation> rval = new LinkedList<BeepsterLocation>();
			PreparedStatement sqlGetGPSLocations = connection
					.prepareStatement(loadStatement(SQL_GET_GPS_LOCATIONS));
			try {
				ResultSet resultSet = sqlGetGPSLocations.executeQuery();
				try {
					while (resultSet.next()) {
						rval.add(new BeepsterLocation(resultSet
								.getTimestamp("b_time"), resultSet
								.getDouble("b_lat"), resultSet
								.getDouble("b_lon"), resultSet
								.getDouble("b_alt"), resultSet
								.getDouble("b_speed"), true));
					}
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetGPSLocations.close();
			}

			return rval;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<BeepsterLocation> getNetworkLocations() {
		return Collections.emptyList();
	}

	public final BeepsterLocation getLastLocation() {
		try {
			PreparedStatement sqlGetLastLocation = connection
					.prepareStatement(loadStatement(SQL_GET_LAST_LOCATION));
			try {
				ResultSet resultSet = sqlGetLastLocation.executeQuery();
				try {
					if (resultSet.next())
						return new BeepsterLocation(
								resultSet.getTimestamp("b_time"),
								resultSet.getDouble("b_lat"),
								resultSet.getDouble("b_lon"),
								resultSet.getDouble("b_alt"),
								resultSet.getDouble("b_speed"), true);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetLastLocation.close();
			}

			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void clearLocations() {
		System.out.println("CLEARING LOCATIONS");
		try {
			PreparedStatement sqlClearLocations = connection
					.prepareStatement(loadStatement(SQL_CLEAR_LOCATIONS));
			try {
				sqlClearLocations.execute();
			} finally {
				sqlClearLocations.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		System.out.println("CLEARING LOCATIONS DONE");
	}

	public final void deleteLocation(long time) {
		try {
			PreparedStatement sqlDeleteLocation = connection
					.prepareStatement(loadStatement(SQL_DELETE_LOCATION));
			try {
				sqlDeleteLocation.setTimestamp(1, new Timestamp(time));
			} finally {
				sqlDeleteLocation.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final void postEvent(int event) {
		postEvent(event, System.currentTimeMillis());
	}

	public final void postEvent(int event, long ms) {
		try {
			PreparedStatement sqlPostEvent = connection
					.prepareStatement(loadStatement(SQL_POST_EVENT));
			try {
				sqlPostEvent.setTimestamp(1, new Timestamp(ms));
				sqlPostEvent.setInt(2, event);
				sqlPostEvent.execute();
			} finally {
				sqlPostEvent.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private final Map.Entry<Date, Integer> makePair(final Date date,
			final Integer event) {

		return new Map.Entry<Date, Integer>() {
			@Override
			public Date getKey() {
				return date;
			}

			@Override
			public Integer getValue() {
				return event;
			}

			@Override
			public Integer setValue(Integer value) {
				throw new UnsupportedOperationException();
			}
		};
	}

	public final LinkedList<Map.Entry<Date, Integer>> getEvents() {

		try {
			LinkedList<Map.Entry<Date, Integer>> rval = new LinkedList<Map.Entry<Date, Integer>>();
			PreparedStatement sqlGetEvents = connection
					.prepareStatement(loadStatement(SQL_GET_EVENTS));
			try {
				ResultSet resultSet = sqlGetEvents.executeQuery();
				try {
					while (resultSet.next())
						rval.add(makePair(resultSet.getTimestamp("b_time"),
								resultSet.getInt("b_event")));

					return rval;
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetEvents.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final Map.Entry<Date, Integer> getLastEvent(int type) {

		try {
			PreparedStatement sqlGetLastEvent = connection
					.prepareStatement(loadStatement(SQL_GET_LAST_EVENT));
			try {
				sqlGetLastEvent.setInt(1, type);
				ResultSet resultSet = sqlGetLastEvent.executeQuery();
				try {
					if (resultSet.next())
						return makePair(resultSet.getTimestamp("b_time"),
								resultSet.getInt("b_event"));
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetLastEvent.close();
			}

			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final boolean isOn(int onEvent, int offEvent) {

		Map.Entry<Date, Integer> lastOn = getLastEvent(onEvent);
		Map.Entry<Date, Integer> lastOff = getLastEvent(offEvent);
		if (null != lastOn) {
			if (null == lastOff
					|| lastOff.getKey().getTime() < lastOn.getKey().getTime())
				return true;
			return false;
		}

		return false;
	}

	@Override
	public final void clearEvents() {
		try {
			PreparedStatement sqlClearEvents = connection
					.prepareStatement(loadStatement(SQL_CLEAR_EVENTS));
			try {
				sqlClearEvents.execute();
			} finally {
				sqlClearEvents.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setInboundBytesLimit(long inboundBytes) {
		try {
			PreparedStatement sqlSetInboundBytesLimit = connection
					.prepareStatement(loadStatement(SQL_SET_INBOUND_BYTES_LIMIT));
			try {
				sqlSetInboundBytesLimit.setLong(1, inboundBytes);
				sqlSetInboundBytesLimit.execute();
			} finally {
				sqlSetInboundBytesLimit.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public long getInboundBytesLimit() {
		try {
			PreparedStatement sqlGetInboundBytesLimit = connection
					.prepareStatement(loadStatement(SQL_GET_INBOUND_BYTES_LIMIT));
			try {
				ResultSet resultSet = sqlGetInboundBytesLimit.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getLong(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetInboundBytesLimit.close();
			}

			return 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setOutboundBytesLimit(long outboundBytes) {
		try {
			PreparedStatement sqlSetOutboundBytesLimit = connection
					.prepareStatement(loadStatement(SQL_SET_OUTBOUND_BYTES_LIMIT));
			try {
				sqlSetOutboundBytesLimit.setLong(1, outboundBytes);
				sqlSetOutboundBytesLimit.execute();
			} finally {
				sqlSetOutboundBytesLimit.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public long getOutboundBytesLimit() {
		try {
			PreparedStatement sqlGetOutboundBytesLimit = connection
					.prepareStatement(loadStatement(SQL_GET_OUTBOUND_BYTES_LIMIT));
			try {
				ResultSet resultSet = sqlGetOutboundBytesLimit.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getLong(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetOutboundBytesLimit.close();
			}

			return 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setInboundBytes(long inboundBytes) {

		try {
			PreparedStatement sqlSetInboundBytes = connection
					.prepareStatement(loadStatement(SQL_SET_INBOUND_BYTES));
			try {
				sqlSetInboundBytes.setLong(1, inboundBytes);
				sqlSetInboundBytes.execute();
			} finally {
				sqlSetInboundBytes.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public long getInboundBytes() {
		try {
			PreparedStatement sqlGetInboundBytes = connection
					.prepareStatement(loadStatement(SQL_GET_INBOUND_BYTES));
			try {
				ResultSet resultSet = sqlGetInboundBytes.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getLong(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetInboundBytes.close();
			}

			return 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setOutboundBytes(long outboundBytes) {

		try {
			PreparedStatement sqlSetOutboundBytes = connection
					.prepareStatement(loadStatement(SQL_SET_OUTBOUND_BYTES));
			try {
				sqlSetOutboundBytes.setLong(1, outboundBytes);
				sqlSetOutboundBytes.execute();
			} finally {
				sqlSetOutboundBytes.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public long getOutboundBytes() {

		try {
			PreparedStatement sqlGetOutboundBytes = connection
					.prepareStatement(loadStatement(SQL_GET_OUTBOUND_BYTES));
			try {
				ResultSet resultSet = sqlGetOutboundBytes.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getLong(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetOutboundBytes.close();
			}

			return 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int getUserId() {

		try {
			PreparedStatement sqlGetUserId = connection
					.prepareStatement(loadStatement(SQL_GET_USER_ID));
			try {
				ResultSet resultSet = sqlGetUserId.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getInt(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetUserId.close();
			}

			return -1;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isRealTimeTrack() {
		try {
			PreparedStatement sqlGetIsRealTimeTrack = connection
					.prepareStatement(loadStatement(SQL_GET_IS_REAL_TIME_TRACK));
			try {
				ResultSet resultSet = sqlGetIsRealTimeTrack.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getInt(1) != 0;
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetIsRealTimeTrack.close();
			}

			return false;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setIsRealTimeTrack(boolean isRealTimeTrack) {

		try {
			PreparedStatement sqlSetIsRealTimeTrack = connection
					.prepareStatement(loadStatement(SQL_SET_IS_REAL_TIME_TRACK));
			try {
				sqlSetIsRealTimeTrack.setLong(1, isRealTimeTrack ? 1 : 0);
				sqlSetIsRealTimeTrack.execute();
			} finally {
				sqlSetIsRealTimeTrack.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setUserOperator(boolean isOperator) {

		try {
			PreparedStatement sqlSetUserOperator = connection
					.prepareStatement(loadStatement(SQL_SET_USER_OPERATOR));
			try {
				sqlSetUserOperator.setLong(1, isOperator ? 1 : 0);
				sqlSetUserOperator.execute();
			} finally {
				sqlSetUserOperator.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isUserOperator() {

		try {
			PreparedStatement sqlGetUserOperator = connection
					.prepareStatement(loadStatement(SQL_GET_USER_OPERATOR));
			try {
				ResultSet resultSet = sqlGetUserOperator.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getInt(1) != 0;
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetUserOperator.close();
			}

			return false;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setUserId(int userId) {

		try {
			PreparedStatement sqlSetUserId = connection
					.prepareStatement(loadStatement(SQL_SET_USER_ID));
			try {
				sqlSetUserId.setLong(1, userId);
				sqlSetUserId.execute();
			} finally {
				sqlSetUserId.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String getUserName() {
		try {
			PreparedStatement sqlGetUserName = connection
					.prepareStatement(loadStatement(SQL_GET_USER_NAME));
			try {
				ResultSet resultSet = sqlGetUserName.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getString(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetUserName.close();
			}

			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setUserName(String userName) {

		try {
			PreparedStatement sqlSetUserName = connection
					.prepareStatement(loadStatement(SQL_SET_USER_NAME));
			try {
				sqlSetUserName.setString(1, userName);
				sqlSetUserName.execute();
			} finally {
				sqlSetUserName.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String getUserEmail() {

		try {
			PreparedStatement sqlGetUserEmail = connection
					.prepareStatement(loadStatement(SQL_GET_USER_EMAIL));
			try {
				ResultSet resultSet = sqlGetUserEmail.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getString(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetUserEmail.close();
			}

			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setUserEmail(String userEmail) {

		try {
			PreparedStatement sqlSetUserEmail = connection
					.prepareStatement(loadStatement(SQL_SET_USER_EMAIL));
			try {
				sqlSetUserEmail.setString(1, userEmail);
				sqlSetUserEmail.execute();
			} finally {
				sqlSetUserEmail.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void setConnectTimeout(int timeout) {

		try {
			PreparedStatement sqlSetConnectTimeout = connection
					.prepareStatement(loadStatement(SQL_SET_CONNECT_TIMEOUT));
			try {
				sqlSetConnectTimeout.setLong(1, timeout);
				sqlSetConnectTimeout.execute();
			} finally {
				sqlSetConnectTimeout.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int getConnectTimeout() {

		try {
			PreparedStatement sqlGetConnectTimeout = connection
					.prepareStatement(loadStatement(SQL_GET_CONNECT_TIMEOUT));
			try {
				ResultSet resultSet = sqlGetConnectTimeout.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getInt(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetConnectTimeout.close();
			}

			return 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] getPhonebookMD5() {

		try {
			MessageDigest messageDigest = MessageDigest.getInstance("MD5");
			for (BeepsterUser user : getPhonebook()) {
				messageDigest.update(user.getEmail().getBytes("utf-8"));
				messageDigest.update(user.getName().getBytes("utf-8"));

				StringBuffer auxPhone1 = new StringBuffer();
				StringBuffer auxPhone2 = new StringBuffer();
				String simPhone = getUserPhones(user, auxPhone1, auxPhone2);

				if (null != simPhone && simPhone.length() > 0)
					messageDigest.update(simPhone.toString().getBytes("utf-8"));

				if (auxPhone1.toString().length() > 0)
					messageDigest
							.update(auxPhone1.toString().getBytes("utf-8"));

				if (auxPhone2.toString().length() > 0)
					messageDigest
							.update(auxPhone2.toString().getBytes("utf-8"));
			}

			return messageDigest.digest();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	public void setUserPhones(BeepsterUser entry, String simPhone,
			String auxPhone1, String auxPhone2) {

		try {
			PreparedStatement sqlSetUserPhones = connection
					.prepareStatement(loadStatement(SQL_SET_USER_PHONES));
			try {
				if (null != simPhone)
					sqlSetUserPhones.setString(1, simPhone.replace("+", "\\+"));
				else
					sqlSetUserPhones.setNull(1, Types.VARCHAR);

				if (null != auxPhone1)
					sqlSetUserPhones
							.setString(2, auxPhone1.replace("+", "\\+"));
				else
					sqlSetUserPhones.setNull(2, Types.VARCHAR);

				if (null != auxPhone2)
					sqlSetUserPhones
							.setString(3, auxPhone2.replace("+", "\\+"));
				else
					sqlSetUserPhones.setNull(3, Types.VARCHAR);

				sqlSetUserPhones.setLong(4, entry.getId());
				sqlSetUserPhones.execute();
			} finally {
				sqlSetUserPhones.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String getUserPhones(BeepsterUser user, StringBuffer auxPhone1,
			StringBuffer auxPhone2) {

		try {
			PreparedStatement sqlGetUserPhones = connection
					.prepareStatement(loadStatement(SQL_GET_USER_PHONES));
			try {
				sqlGetUserPhones.setInt(1, user.getId());
				ResultSet resultSet = sqlGetUserPhones.executeQuery();
				try {
					while (resultSet.next()) {
						String simPhone = resultSet.getString("b_sim_phone");
						if (simPhone != null)
							simPhone = simPhone.replace("\\+", "+");
						if (auxPhone1 != null) {
							String text = resultSet.getString("b_aux_phone1");
							if (text != null)
								auxPhone1.append(text.replace("\\+", "+"));
						}
						if (auxPhone2 != null) {
							String text = resultSet.getString("b_aux_phone2");
							if (text != null)
								auxPhone2.append(text.replace("\\+", "+"));
						}

						return simPhone;
					}
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetUserPhones.close();
			}

			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final static BeepsterUser getUserFromResultSet(ResultSet resultSet)
			throws SQLException {

		BeepsterUser user = new BeepsterUser(
				(int) resultSet.getLong("b_user_id"),
				resultSet.getString("b_name"), resultSet.getString("b_email"));

		user.setOperator(resultSet.getLong("b_operator") != 0);

		user.setSIMPhone(resultSet.getString("b_sim_phone"));
		if (user.getSIMPhone() != null)
			user.setSIMPhone(user.getSIMPhone().replace("\\+", "+"));
		user.setAuxPhone1(resultSet.getString("b_aux_phone1"));
		if (user.getAuxPhone1() != null)
			user.setAuxPhone1(user.getAuxPhone1().replace("\\+", "+"));
		user.setAuxPhone2(resultSet.getString("b_aux_phone2"));
		if (user.getAuxPhone2() != null)
			user.setAuxPhone2(user.getAuxPhone2().replace("\\+", "+"));

		return user;
	}

	public List<BeepsterUser> getPhonebook() throws SQLException {
		LinkedList<BeepsterUser> rval = new LinkedList<BeepsterUser>();
		try {
			PreparedStatement sqlGetPhonebook = connection
					.prepareStatement(loadStatement(SQL_GET_PHONEBOOK));
			try {
				ResultSet resultSet = sqlGetPhonebook.executeQuery();
				try {
					while (resultSet.next()) {
						BeepsterUser user = getUserFromResultSet(resultSet);
						rval.add(user);
					}
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetPhonebook.close();
			}
		} catch (IOException e) {
			throw new SQLException(e);
		}

		return rval;
	}

	public synchronized void clearPhonebook() {

		try {
			PreparedStatement sqlClearPhonebook = connection
					.prepareStatement(loadStatement(SQL_CLEAR_PHONEBOOK));
			try {
				sqlClearPhonebook.execute();
			} finally {
				sqlClearPhonebook.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final BeepsterUser getUserById(long id) {

		try {
			PreparedStatement sqlGetUserById = connection
					.prepareStatement(loadStatement(SQL_GET_USER_BY_ID));
			try {
				sqlGetUserById.setLong(1, id);
				ResultSet resultSet = sqlGetUserById.executeQuery();
				try {
					if (resultSet.next())
						return getUserFromResultSet(resultSet);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetUserById.close();
			}

			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized BeepsterUser addPhonebookEntry(BeepsterUser user) {

		try {
			PreparedStatement sqlInsertPhonebookEntry = connection
					.prepareStatement(loadStatement(SQL_ADD_PHONEBOOK_ENTRY));
			try {
				sqlInsertPhonebookEntry.setLong(1, user.getId());
				sqlInsertPhonebookEntry.setString(2, user.getName());
				sqlInsertPhonebookEntry.setString(3, user.getEmail());
				sqlInsertPhonebookEntry.setLong(4, user.isOperator() ? 1 : 0);
				sqlInsertPhonebookEntry.execute();
				return getUserById(user.getId());
			} finally {
				sqlInsertPhonebookEntry.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void deleteSynchronizedMessages() {

		try {
			PreparedStatement sqlDeleteSynchronizedMessages = connection
					.prepareStatement(loadStatement(SQL_DELETE_SYNCHRONIZED_MESSAGES));
			PreparedStatement sqlDeleteStreamMessages = connection
					.prepareStatement(loadStatement(SQL_DELETE_STREAM_MESSAGES));

			try {
				sqlDeleteSynchronizedMessages.setLong(1,
						BeepsterMessage.MESSAGE_STATUS_NONE);
				sqlDeleteSynchronizedMessages.setLong(2, getUserId());
				sqlDeleteSynchronizedMessages.execute();

				sqlDeleteStreamMessages.setLong(1,
						BeepsterMessage.MESSAGE_TYPE_STREAM);
				sqlDeleteStreamMessages.execute();
			} finally {
				sqlDeleteSynchronizedMessages.close();
				sqlDeleteStreamMessages.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void clearOverdueMessages() {

		try {
			PreparedStatement sqlClearOverdueMessages = connection
					.prepareStatement(loadStatement(SQL_CLEAR_OVERDUE_MESSAGES));
			try {
				sqlClearOverdueMessages.execute();
			} finally {
				sqlClearOverdueMessages.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final BeepsterMessage getMessageByInternalId(int id) {

		try {
			PreparedStatement sqlGetMessageByInternalId = connection
					.prepareStatement(loadStatement(SQL_GET_MESSAGE_BY_INTERNAL_ID));

			try {
				sqlGetMessageByInternalId.setInt(1, id);
				ResultSet resultSet = sqlGetMessageByInternalId.executeQuery();
				try {
					if (resultSet.next())
						return getMessageFromResultSet(resultSet);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetMessageByInternalId.close();
			}

			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final BeepsterMessage getMessageById(int id) {

		try {
			PreparedStatement sqlGetMessageById = connection
					.prepareStatement(loadStatement(SQL_GET_MESSAGE_BY_ID));
			try {
				sqlGetMessageById.setInt(1, id);
				ResultSet resultSet = sqlGetMessageById.executeQuery();
				try {
					if (resultSet.next())
						return getMessageFromResultSet(resultSet);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetMessageById.close();
			}

			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static BeepsterMessage getMessageFromResultSet(ResultSet resultSet) {

		try {
			int type = resultSet.getInt("b_type");

			Blob data = resultSet.getBlob("b_data");
			BeepsterMessage rval;
			try {
				rval = BeepsterMessage.loadMessage(
						new DataInputStream(data.getBinaryStream()), type);
				rval.setType(type);
				rval.setId(resultSet.getInt("b_system_id"));
				rval.setFromId(resultSet.getInt("b_from"));
				rval.setToId(resultSet.getInt("b_to"));
				rval.setStatus(resultSet.getInt("b_status"));
				rval.setSize(resultSet.getInt("b_size"));
				rval.setDateSent(resultSet.getTimestamp("b_time"));

				// the internal database id
				rval.setInternalId(resultSet.getInt("_id"));
				return rval;
			} catch (IOException e) {
				// shouldn't happen
				throw new RuntimeException(e);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public List<BeepsterMessage> getInboxMessages() {

		try {
			LinkedList<BeepsterMessage> rval = new LinkedList<BeepsterMessage>();
			PreparedStatement sqlGetInboxMessages = connection
					.prepareStatement(loadStatement(SQL_GET_INBOX_MESSAGES));
			try {
				ResultSet resultSet = sqlGetInboxMessages.executeQuery();
				try {
					while (resultSet.next()) {
						BeepsterMessage message = BeepsterDatabase
								.getMessageFromResultSet(resultSet);
						rval.add(message);
					}
				} finally {
					resultSet.close();
				}

				return rval;
			} finally {
				sqlGetInboxMessages.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public List<BeepsterMessage> getOutboxMessages() {

		try {
			LinkedList<BeepsterMessage> rval = new LinkedList<BeepsterMessage>();
			PreparedStatement sqlGetOutboxMessages = connection
					.prepareStatement(loadStatement(SQL_GET_OUTBOX_MESSAGES));
			try {
				ResultSet resultSet = sqlGetOutboxMessages.executeQuery();
				try {
					while (resultSet.next()) {
						BeepsterMessage message = BeepsterDatabase
								.getMessageFromResultSet(resultSet);
						rval.add(message);
					}
				} finally {
					resultSet.close();
				}

				return rval;
			} finally {
				sqlGetOutboxMessages.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void addMessage(BeepsterMessage message) {
		try {
			PreparedStatement sqlAddMessage = connection
					.prepareStatement(loadStatement(SQL_ADD_MESSAGE));

			try {
				sqlAddMessage.setLong(1, message.getId());
				sqlAddMessage.setLong(2, message.getType());
				sqlAddMessage.setLong(3, message.getStatus());
				sqlAddMessage.setLong(4, message.getFromId());
				sqlAddMessage.setLong(5, message.getToId());
				sqlAddMessage.setLong(6, message.getSize());

				sqlAddMessage.setTimestamp(7, new Timestamp(message
						.getDateSent().getTime()));

				// message data
				try {
					sqlAddMessage.setBlob(8,
							new ByteArrayInputStream(message.getRawBytes()));
				} catch (IOException e) {
					// shouldn't happen
					throw new RuntimeException(e);
				}

				sqlAddMessage.execute();
			} finally {
				sqlAddMessage.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void updateMessageStatus(BeepsterMessage message) {
		try {
			PreparedStatement sqlUpdateMessageStatus = connection
					.prepareStatement(loadStatement(SQL_UPDATE_MESSAGE_STATUS));
			try {
				sqlUpdateMessageStatus.setLong(1, message.getStatus());
				sqlUpdateMessageStatus.setLong(2, message.getId());
				sqlUpdateMessageStatus.execute();
			} finally {
				sqlUpdateMessageStatus.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void updateMessageId(BeepsterMessage message) {

		try {
			PreparedStatement sqlUpdateMessageId = connection
					.prepareStatement(loadStatement(SQL_UPDATE_MESSAGE_ID));
			try {
				sqlUpdateMessageId.setLong(1, message.getId());
				sqlUpdateMessageId.setLong(2, message.getInternalId());
				sqlUpdateMessageId.execute();
			} finally {
				sqlUpdateMessageId.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized void deleteMessage(BeepsterMessage message) {
		try {
			PreparedStatement sqlDeleteMessage = connection
					.prepareStatement(loadStatement(SQL_DELETE_MESSAGE));
			try {
				sqlDeleteMessage.setLong(1, message.getInternalId());
				sqlDeleteMessage.execute();
			} finally {
				sqlDeleteMessage.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void setSpeedMargin(byte speedMargin) {
		try {
			PreparedStatement setSpeedMargin = connection
					.prepareStatement(loadStatement(SQL_SET_SPEED_MARGIN));
			setSpeedMargin.setByte(1, speedMargin);
			setSpeedMargin.execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public byte getSpeedMargin() {
		try {
			PreparedStatement sqlGetSpeedMargin = connection
					.prepareStatement(loadStatement(SQL_GET_SPEED_MARGIN));
			try {
				ResultSet resultSet = sqlGetSpeedMargin.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getByte(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetSpeedMargin.close();
			}

			return -1;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void setWaitMargin(byte waitMargin) {
		try {
			PreparedStatement setWaitMargin = connection
					.prepareStatement(loadStatement(SQL_SET_WAIT_MARGIN));
			setWaitMargin.setByte(1, waitMargin);
			setWaitMargin.execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public byte getWaitMargin() {
		try {
			PreparedStatement sqlGetWaitMargin = connection
					.prepareStatement(loadStatement(SQL_GET_WAIT_MARGIN));
			try {
				ResultSet resultSet = sqlGetWaitMargin.executeQuery();
				try {
					if (resultSet.next())
						return resultSet.getByte(1);
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetWaitMargin.close();
			}

			return -1;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int getLastStreamValue(int type) {
		try {
			PreparedStatement sqlGetLastStreamValue = connection
					.prepareStatement(loadStatement(SQL_GET_LAST_STREAM_VALUE));
			try {
				sqlGetLastStreamValue.setInt(1, type);
				ResultSet resultSet = sqlGetLastStreamValue.executeQuery();
				try {
					if (resultSet.next())
						return (int) resultSet.getInt("b_value");
				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetLastStreamValue.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return 0;
	}

	public void addStreamValue(int type, int newVal, long lastUpdated) {
		// 0 delta can't be encoded
		if (getLastStreamValue(type) == newVal)
			return;

		try {
			PreparedStatement sqlAddStreamValue = connection
					.prepareStatement(loadStatement(SQL_ADD_STREAM_VALUE));
			try {
				sqlAddStreamValue.setLong(1, type);
				sqlAddStreamValue.setLong(2, newVal);
				sqlAddStreamValue.setTimestamp(3, new Timestamp(lastUpdated));
				sqlAddStreamValue.execute();
			} finally {
				sqlAddStreamValue.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void deleteStreamValues(int type) {
		try {
			PreparedStatement sqlDeleteStreamValues = connection
					.prepareStatement(loadStatement(SQL_DELETE_STREAM_VALUES));
			try {
				sqlDeleteStreamValues.setLong(1, type);
				sqlDeleteStreamValues.execute();
			} finally {
				sqlDeleteStreamValues.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public List<Map.Entry<Integer, Long>> getStreamValues(int type) {
		LinkedList<Map.Entry<Integer, Long>> rval = new LinkedList<Map.Entry<Integer, Long>>();

		try {
			PreparedStatement sqlGetStreamValues = connection
					.prepareStatement(loadStatement(SQL_GET_STREAM_VALUES));
			try {
				sqlGetStreamValues.setInt(1, type);
				ResultSet resultSet = sqlGetStreamValues.executeQuery();
				try {
					while (resultSet.next()) {
						final int key = resultSet.getInt("b_value");
						final long value = resultSet.getTimestamp("b_time")
								.getTime();
						rval.add(new Map.Entry<Integer, Long>() {
							@Override
							public Integer getKey() {
								return key;
							}

							@Override
							public Long getValue() {
								return value;
							}

							@Override
							public Long setValue(Long object) {
								throw new UnsupportedOperationException();
							}
						});
					}

				} finally {
					resultSet.close();
				}
			} finally {
				sqlGetStreamValues.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return rval;
	}

	public void packECUValues() {
		try {
			for (BeepsterUser user : BeepsterConsole.INSTANCE.getDatabase()
					.getPhonebook()) {
				// don't send to non-operators
				if (!user.isOperator())
					continue;

				List<SubStream> subStreams = new LinkedList<SubStream>();
				SubStream subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_RPM);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_COOLANT_TEMPERATURE);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_FUEL_TANK_1);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_FUEL_TANK_2);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_TRIP_DISTANCE);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_TOTAL_DISTANCE);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_TACHOGRAPH_SPEED);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_ACCELERATOR_PEDAL_POSITION);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_ECU_TOTAL_FUEL_USED);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_SUPPLY_VOLTAGE);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_BATTERY_VOLTAGE);
				if (null != subStream)
					subStreams.add(subStream);
				subStream = doPackECUValues((byte) BeepsterStreamMessage.TYPE_BATTERY_CHARGE_PERCENTAGE);
				if (null != subStream)
					subStreams.add(subStream);

				// no values to add
				if (subStreams.isEmpty())
					return;

				SubStream[] array = new SubStream[subStreams.size()];
				BeepsterStreamMessage message = new BeepsterStreamMessage(
						getUserId(), user.getId(), subStreams.toArray(array));
				addMessage(message);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private SubStream doPackECUValues(byte type) {
		List<Map.Entry<Integer, Long>> values = BeepsterConsole.INSTANCE
				.getDatabase().getStreamValues(type);

		// no values to pack
		if (values.isEmpty())
			return null;

		int[] intValues = new int[values.size()];
		int[] dateValues = new int[values.size()];
		for (int index = 0; index < values.size(); index++) {
			intValues[index] = values.get(index).getKey();

			dateValues[index] = (int) (values.get(index).getValue() / 1000);
		}
		byte[] ival = BeepsterStreamMessage.encodeSequence(intValues);
		byte[] dval = BeepsterStreamMessage.encodeSequence(dateValues);

		deleteStreamValues(type);
		return new SubStream(type, ival, dval);
	}

	@Override
	public void beginTransaction() {
		try {
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void commitTransaction() {
		try {
			try {
				connection.commit();
			} finally {
				connection.setAutoCommit(true);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void rollbackTransaction() {
		try {
			try {
				connection.rollback();
			} finally {
				connection.setAutoCommit(true);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
