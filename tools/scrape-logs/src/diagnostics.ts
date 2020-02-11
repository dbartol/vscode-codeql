export function reportError(lineNumber: number, message: string): never {
  throw new Error(`Line ${lineNumber}: ${message}`);
}
