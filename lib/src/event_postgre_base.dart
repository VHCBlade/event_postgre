import 'dart:async';

import 'package:event_bloc/event_bloc.dart';
import 'package:event_db/event_db.dart';
import 'package:event_postgre/src/postgre_extension.dart';
import 'package:postgres/postgres.dart';

/// Provides a [PostgreSQLConnection] on demand.
typedef PostgreSQLConnectionSource = FutureOr<PostgreSQLConnection> Function();

/// Implementation that connects to a post gre database.
///
/// [getConnection] must be provided through the constructor
class PostgreDatabase extends DatabaseRepository {
  /// [getConnection] provides the connection to the postGreSQLConnection to be
  /// used for the transactions
  PostgreDatabase(this.getConnection, this.modelSupplierMap);

  /// provides the connection to be used in the database transactions
  final PostgreSQLConnectionSource getConnection;

  /// Provides the suppliers for GenericModels
  final Map<Type, BaseModel Function()> modelSupplierMap;

  @override
  FutureOr<bool> deleteModel<T extends BaseModel>(
    String database,
    T model,
  ) async {
    if (model.id == null) {
      return false;
    }
    final statement =
        'DELETE FROM $database.${model.postGreTable} WHERE id = @id';

    final substitution = {'id': model.id};
    final connection = await getConnection();

    final result =
        await connection.query(statement, substitutionValues: substitution);

    return result.affectedRowCount > 0;
  }

  @override
  FutureOr<Iterable<T>> findAllModelsOfType<T extends BaseModel>(
    String database,
    T Function() supplier,
  ) async {
    final sampleModel = supplier();
    final statement = 'SELECT json FROM $database.${sampleModel.postGreTable}';

    final connection = await getConnection();

    final result = await connection.query(statement);

    return result.map((e) => supplier()..loadFromPostGreRow(e));
  }

  @override
  FutureOr<T?> findModel<T extends BaseModel>(
    String database,
    String key,
  ) async {
    final modelSupplier = modelSupplierMap[T];
    assert(
      modelSupplier != null,
      '$T is not available in the modelSupplierMap. '
      'Please ensure you add all models you want to use.',
    );

    final sampleModel = modelSupplier!() as T;
    final statement = 'SELECT json FROM  $database.${sampleModel.postGreTable}'
        ' WHERE id = @id';

    final substitution = {'id': key};
    final connection = await getConnection();

    final result =
        await connection.query(statement, substitutionValues: substitution);

    if (result.isEmpty) {
      return null;
    }

    return sampleModel..loadFromPostGreRow(result.first);
  }

  @override
  FutureOr<Iterable<T>> findModels<T extends BaseModel>(
    String database,
    Iterable<String> keys,
  ) async {
    T supplier() => modelSupplierMap[T]!() as T;
    final statement = 'SELECT json FROM $database.${supplier().postGreTable}'
        ' WHERE id = ANY(@ids)';

    final substitution = {'ids': keys.toList()};
    final connection = await getConnection();

    final result =
        await connection.query(statement, substitutionValues: substitution);

    return result.map((e) => supplier()..loadFromPostGreRow(e));
  }

  @override
  FutureOr<Iterable<T>> searchByModelAndFields<T extends BaseModel>(
    String database,
    T Function() supplier,
    T model,
    List<String> fields,
  ) async {
    final initialStatement =
        'SELECT json FROM  $database.${supplier().postGreTable}';

    var initial = true;
    final statementBuffer = StringBuffer(initialStatement);
    final substitutionValues = <String, dynamic>{};

    for (final field in fields) {
      final value = model.getField(field);
      substitutionValues[field] = value;

      statementBuffer.write(initial ? ' WHERE ' : ' AND ');
      initial = false;

      statementBuffer.write('$field = @$field');
    }

    final connection = await getConnection();

    final result = await connection.query(
      statementBuffer.toString(),
      substitutionValues: substitutionValues,
    );

    return result.map((e) => supplier()..loadFromPostGreRow(e));
  }

  @override
  FutureOr<T> saveModel<T extends BaseModel>(
    String database,
    T model,
  ) async {
    while (model.id == null) {
      if (await findModel<T>(database, model.autoGenId) != null) {
        model.id = null;
      }
    }
    final table = '$database.${model.postGreTable}';

    final statement = 'INSERT INTO $table (json) VALUES (@jsonValue) '
        'ON CONFLICT (id) DO UPDATE SET json = EXCLUDED.json';
    final substitutionValues = <String, dynamic>{
      'jsonValue': model.toJsonString(),
    };

    final connection = await getConnection();

    await connection.query(
      statement,
      substitutionValues: substitutionValues,
    );

    return model;
  }

  @override
  FutureOr<bool> containsRows<T extends BaseModel>(
    String database,
    T Function() supplier,
  ) async {
    final table = '$database.${supplier().postGreTable}';

    final statement = 'SELECT EXISTS (SELECT 1 FROM $table) AS hasrows';

    final connection = await getConnection();

    final result = await connection.query(statement);

    return result[0].toColumnMap()['hasrows'] as bool;
  }

  @override
  List<BlocEventListener<dynamic>> generateListeners(
    BlocEventChannel channel,
  ) =>
      [];
}
