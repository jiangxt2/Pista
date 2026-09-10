# Error Reference

Pista errors are defined in `pista-common/src/main/resources/error/error-classes.json` and constructed through `PistaErrors` or specialized error helpers.

## Contract

An error definition contains a stable error class and a parameterized message. Public error classes should identify the failed capability without exposing credentials, complete SQL text, connection strings, or business data.

## Handling

- Configuration and capability failures should occur before external side effects.
- Spark exceptions should preserve the Spark error class and root cause where possible.
- Connector errors should identify the target operation and recovery action without logging secrets.
- A continue policy must record the failure in the execution report.
- Unknown remote commit outcomes must not be rewritten as success.

## Adding an error

1. Add or reuse a stable entry in the error catalog.
2. Add a typed construction method.
3. Preserve the original cause.
4. Add tests for the error class, parameters, message loading, and redaction.
5. Update public documentation when the error is user-facing.

## Compatibility

Renaming, merging, or deleting an error class requires an explicit compatibility decision. Do not reuse an existing error class for an unrelated condition.

The generated catalog is authoritative for the complete current inventory.
