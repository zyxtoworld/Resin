/**
 * Resin currently runs as one stateful process/container. A process restart
 * rotates the in-memory cursor signing key, so the API deliberately returns
 * 400 for an old cursor. The UI must recover by starting at page one.
 */
export function shouldResetLeaseCursorOnError(status: number | undefined, cursor: string): boolean {
  return status === 400 && cursor.trim().length > 0;
}
