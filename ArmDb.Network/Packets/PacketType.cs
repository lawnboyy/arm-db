namespace ArmDb.Network;

/// <summary>
/// Defines the message types for the ArmDb wire protocol.
/// These are stored as the first byte of every packet.
/// </summary>
public enum PacketType : byte
{
  // --- Client -> Server Messages ---

  /// <summary>
  /// 'C': Initiate a connection. Payload contains protocol version.
  /// </summary>
  Connect = (byte)'C',

  /// <summary>
  /// 'Q': Execute a simple SQL query. Payload is the SQL string.
  /// </summary>
  Query = (byte)'Q',

  /// <summary>
  /// 'X': Terminate the connection gracefully. No payload.
  /// </summary>
  Terminate = (byte)'X',

  // --- Server -> Client Messages ---

  /// <summary>
  /// 'R': Authentication/Connection successful.
  /// </summary>
  AuthenticationOk = (byte)'R',

  /// <summary>
  /// 'E': An error occurred. Payload contains severity and message.
  /// </summary>
  Error = (byte)'E',

  /// <summary>
  /// 'T': Row Description. Contains metadata (column names/types) for a result set.
  /// </summary>
  RowDescription = (byte)'T',

  /// <summary>
  /// 'D': Data Row. Contains the actual values for a single row.
  /// </summary>
  DataRow = (byte)'D',

  /// <summary>
  /// 'C': Command Complete. Signals the end of a SQL command execution (e.g., "INSERT 1").
  /// </summary>
  CommandComplete = (byte)'F',

  /// <summary>
  /// 'Z': Ready For Query. Signals the server is idle and ready for the next command.
  /// Payload: Transaction status (I=Idle, T=InTransaction, E=Error).
  /// </summary>
  ReadyForQuery = (byte)'Z'
}