import { Readable } from 'stream';
import { StringDecoder } from 'string_decoder';

/**
 * Buffer to hold state used when splitting a text stream into lines.
 */
class SplitBuffer {
  private readonly decoder = new StringDecoder('utf8');
  private readonly maxSeparatorLength: number;
  private buffer = '';
  private searchIndex = 0;

  constructor(private readonly separators: readonly string[]) {
    this.maxSeparatorLength = separators.map(s => s.length).reduce((a, b) => Math.max(a, b), 0);
  }

  /**
   * Append new text data to the buffer.
   * @param chunk The chunk of data to append.
   */
  public addChunk(chunk: Buffer): void {
    this.buffer += this.decoder.write(chunk);
  }

  /**
   * Signal that the end of the input stream has been reached.
   */
  public end(): void {
    this.buffer += this.decoder.end();
    this.buffer += this.separators[0];  // Append a separator to the end to ensure the last line is returned.
  }

  /**
   * Extract the next full line from the buffer, if one is available.
   * @returns The text of the next available full line (without the separator), or `undefined` if no
   * line is available.
   */
  public getNextLine(): string | undefined {
    while (this.searchIndex <= (this.buffer.length - this.maxSeparatorLength)) {
      for (const separator of this.separators) {
        if (this.buffer.startsWith(separator, this.searchIndex)) {
          const line = this.buffer.substr(0, this.searchIndex);
          this.buffer = this.buffer.substr(this.searchIndex + separator.length);
          this.searchIndex = 0;
          return line;
        }
      }
      this.searchIndex++;
    }

    return undefined;
  }
}

/**
 * Splits a text stream into lines based on a list of valid line separators.
 * @param stream The text stream to split. This stream will be fully consumed.
 * @param separators The list of strings that act as line separators.
 * @returns A sequence of lines (not including separators).
 */
export async function* splitStreamAtSeparators(stream: Readable, separators: string[]):
  AsyncGenerator<string, void, unknown> {

  const buffer = new SplitBuffer(separators);
  for await (const chunk of stream) {
    buffer.addChunk(chunk);
    let line: string | undefined;
    do {
      line = buffer.getNextLine();
      if (line !== undefined) {
        yield line;
      }
    } while (line !== undefined);
  }
  buffer.end();
  let line: string | undefined;
  do {
    line = buffer.getNextLine();
    if (line !== undefined) {
      yield line;
    }
  } while (line !== undefined);
}

/**
 *  Standard line endings for splitting human-readable text.
 */
export const lineEndings = ['\r\n', '\r', '\n'];
