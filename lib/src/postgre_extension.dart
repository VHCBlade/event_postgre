import 'package:event_db/event_db.dart';
import 'package:postgres/postgres.dart';

/// Adds convenience functions for a postGreModel
extension PostGreModel on BaseModel {
  /// Returns the expected table for postgre based on the [type]
  String get postGreTable => '${type.toLowerCase()}s';

  /// Loads the given [row] into the data of this model
  void loadFromPostGreRow(PostgreSQLResultRow row) => loadFromMap(
        row.toColumnMap()['json'] as Map<String, dynamic>,
      );
}
