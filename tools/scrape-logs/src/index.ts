import * as fs from 'fs-extra'
import * as yargs from 'yargs'
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
async function* splitStreamAtSeparators(stream: Readable, separators: string[]):
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
const lineEndings = ['\r\n', '\r', '\n'];

interface RAInstruction {
  operation: string;
  tupleCount?: number;
  duplication?: number;
}

enum ProfileEventKind {
  PREDICATE_EVALUATION = 'predicate_evaluation',
  RECURSIVE_PREDICATE_EVALUATION = 'recursive_predicate_evaluation',
  DATABASE_TABLE_EVALUATION = 'database_table_evaluation'
}

interface PredicateEvaluationEvent {
  kind: ProfileEventKind.PREDICATE_EVALUATION;
  predicate: string;
  arity: number;
  hash: string;
  instructions: RAInstruction[];
  rows: number,
  columns: number
}

interface EvaluationIteration {
  index: number;
  rows: number;
  order: string;
  instructions: RAInstruction[];
}

interface RecursivePredicateEvaluationEvent {
  kind: ProfileEventKind.RECURSIVE_PREDICATE_EVALUATION;
  predicate: string;
  arity: number;
  hash: string;
  iterations: EvaluationIteration[];
}

interface DatabaseTableEvaluationEvent {
  kind: ProfileEventKind.DATABASE_TABLE_EVALUATION;
  predicate: string;
  rows: number;
  columns: number;
}

type ProfileEvent = PredicateEvaluationEvent | RecursivePredicateEvaluationEvent | DatabaseTableEvaluationEvent;

enum ProfileLineKind {
  END_OF_FILE = 'end_of_file',
  START_PREDICATE_EVALUATION = 'start_predicate_evaluation',
  TUPLE_COUNTS = 'tuple_counts',
  RA_INSTRUCTION = 'ra_instruction',
  WROTE_RELATION = 'wrote_relation',
  DELTA_ROW_COUNT = 'delta_row_count',
  ACCUMULATING_DELTAS = 'accumulating_deltas'
}

interface ProfileLineBase {
  lineNumber: number;
}

interface EndOfFileLine extends ProfileLineBase {
  kind: ProfileLineKind.END_OF_FILE;
}

interface StartPredicateEvaluationLine extends ProfileLineBase {
  kind: ProfileLineKind.START_PREDICATE_EVALUATION;
  predicate: string;
  arity: number;
  iteration: number | undefined;
  hash: string;
}

interface TupleCountsLine extends ProfileLineBase {
  kind: ProfileLineKind.TUPLE_COUNTS;
  predicate: string;
}

interface RAInstructionLine extends ProfileLineBase {
  kind: ProfileLineKind.RA_INSTRUCTION;
  operation: string;
  tupleCount?: number;
  duplication?: number;
}

interface WroteRelationLine extends ProfileLineBase {
  kind: ProfileLineKind.WROTE_RELATION;
  predicate: string;
  rows: number;
  columns: number;
}

interface DeltaRowCountLine extends ProfileLineBase {
  kind: ProfileLineKind.DELTA_ROW_COUNT;
  predicate: string;
  order: string;
  rows: number;
}

interface AccumulatingDeltasLine extends ProfileLineBase {
  kind: ProfileLineKind.ACCUMULATING_DELTAS;
}

type ProfileLine = EndOfFileLine | StartPredicateEvaluationLine | TupleCountsLine | RAInstructionLine | WroteRelationLine |
  DeltaRowCountLine | AccumulatingDeltasLine;

function reportError(lineNumber: number, message: string): never {
  throw new Error(`Line ${lineNumber}: ${message}`);
}

const timestampRegex = /\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] \(\d+s\)/;

function timestampedLineRegex(_pattern: RegExp): RegExp {
  return new RegExp(`^(${timestampRegex.source}) (?:${_pattern.source})`);
}

const startEvaluatingPredicateLineRegex = timestampedLineRegex(
  /Starting to evaluate predicate ([^/]+)\/(\d+)(?:\[(\d+)\])?@([A-Fa-f0-9]+)(?: \(iteration \d+\))?/
);
const tupleCountsLineRegex = timestampedLineRegex(/Tuple counts for (.*)+:/);
const nonReturnLineRegex = /^ +(\d+) +~(\d+)% +(.*)$/;
const returnLineRegex = /^ +(return .*)$/;
const wroteRelationLineRegex = timestampedLineRegex(/ >>> Wrote relation ([^ ]+) with (\d+) rows and (\d+) columns\./);
const deltaRowCountLineRegex = timestampedLineRegex(/\s+- ([^ ]+) has (\d+) rows \(order for disjuncts: delta=(\d+|\(\<standard\>)\)\./);
const emptyDeltaLineRegex = timestampedLineRegex(/Empty delta for ([^ ]+) \(order for disjuncts: delta=(\d+|\(<standard>)\)\./);
const accumulatingDeltasLineRegex = timestampedLineRegex(/Accumulating deltas/);

class LineBuffer {
  private nextLine: ProfileLine | undefined = undefined;
  private lineNumber = 0;
  private generator: AsyncGenerator<string, void, unknown>;

  constructor(private readonly stream: Readable) {
    this.generator = splitStreamAtSeparators(this.stream, lineEndings);
  }

  public async peek(): Promise<ProfileLine> {
    while (this.nextLine === undefined) {
      const lineResult = await this.generator.next();
      this.lineNumber++;
      if (lineResult.done) {
        this.nextLine = {
          kind: ProfileLineKind.END_OF_FILE,
          lineNumber: this.lineNumber
        };
      }
      else {
        const line = <string>lineResult.value;

        let match: RegExpMatchArray | null;
        if (match = line.match(startEvaluatingPredicateLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.START_PREDICATE_EVALUATION,
            lineNumber: this.lineNumber,
            predicate: match[2],
            arity: parseInt(match[3]),
            iteration: (match[4] !== undefined) ? parseInt(match[4]) : undefined,
            hash: match[5]
          };
        }
        else if (match = line.match(tupleCountsLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.TUPLE_COUNTS,
            lineNumber: this.lineNumber,
            predicate: match[2]
          };
        }
        else if (match = line.match(nonReturnLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.RA_INSTRUCTION,
            lineNumber: this.lineNumber,
            tupleCount: parseInt(match[2]),
            duplication: parseInt(match[3]),
            operation: match[4]
          };
        }
        else if (match = line.match(returnLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.RA_INSTRUCTION,
            lineNumber: this.lineNumber,
            operation: match[2]
          };
        }
        else if (match = line.match(wroteRelationLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.WROTE_RELATION,
            lineNumber: this.lineNumber,
            predicate: match[2],
            rows: parseInt(match[3]),
            columns: parseInt(match[4])
          };
        }
        else if (match = line.match(deltaRowCountLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.DELTA_ROW_COUNT,
            lineNumber: this.lineNumber,
            predicate: match[2],
            rows: parseInt(match[3]),
            order: match[4]
          };
        }
        else if (match = line.match(emptyDeltaLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.DELTA_ROW_COUNT,
            lineNumber: this.lineNumber,
            predicate: match[2],
            rows: 0,
            order: match[3]
          };
        }
        else if (match = line.match(accumulatingDeltasLineRegex)) {
          this.nextLine = {
            kind:ProfileLineKind.ACCUMULATING_DELTAS,
            lineNumber: this.lineNumber
          };
        }
      }
    }

    return this.nextLine;
  }

  public async read(): Promise<ProfileLine> {
    const line = await this.peek();
    this.nextLine = undefined;
    return line;
  }

  public async expect(kind: ProfileLineKind): Promise<ProfileLine> {
    const line = await this.read();
    if (line.kind === kind) {
      return line;
    }
    else {
      reportError(line.lineNumber, `Expected token '${kind}'.`);
    }
  }
}

class Parser {
  constructor(private readonly lineBuffer: LineBuffer) {
  }

  public async parseEvent(): Promise<ProfileEvent | undefined> {
    const firstLine = await this.lineBuffer.peek();
    switch (firstLine.kind) {
      case ProfileLineKind.END_OF_FILE:
        await this.lineBuffer.read();
        return undefined;

      case ProfileLineKind.START_PREDICATE_EVALUATION:
        if (firstLine.iteration !== undefined) {
          return await this.parseRecursivePredicateEvaluation();
        }
        else {
          return await this.parsePredicateEvaluation();
        }

      case ProfileLineKind.WROTE_RELATION:
        return await this.parseDatabaseTableEvaluation();

      default:
        await this.lineBuffer.read();
        reportError(firstLine.lineNumber, `Unexpected line: '${firstLine.kind}'.`);
    }
  }

  private async parseDatabaseTableEvaluation(): Promise<DatabaseTableEvaluationEvent> {
    const line = <WroteRelationLine>await this.lineBuffer.expect(ProfileLineKind.WROTE_RELATION);
    return {
      kind: ProfileEventKind.DATABASE_TABLE_EVALUATION,
      predicate: line.predicate,
      rows: line.rows,
      columns: line.columns
    };
  }

  private async parsePredicateEvaluation(): Promise<PredicateEvaluationEvent> {
    const startLine = <StartPredicateEvaluationLine>await this.lineBuffer.expect(ProfileLineKind.START_PREDICATE_EVALUATION);

    const tupleCountLine = <TupleCountsLine>await this.lineBuffer.expect(ProfileLineKind.TUPLE_COUNTS);
    if (tupleCountLine.predicate !== startLine.predicate) {
      reportError(tupleCountLine.lineNumber, `Mismatched predicate: expected '${startLine.predicate}', but got '${tupleCountLine.predicate}'.`);
    }

    const instructions = await this.parseRAInstructions();

    const wroteRelationLine = <WroteRelationLine>await this.lineBuffer.expect(ProfileLineKind.WROTE_RELATION);
    if (wroteRelationLine.predicate !== startLine.predicate) {
      reportError(wroteRelationLine.lineNumber, `Mismatched predicate: expected '${startLine.predicate}', but got '${wroteRelationLine.predicate}'.`);
    }

    return {
      kind: ProfileEventKind.PREDICATE_EVALUATION,
      predicate: startLine.predicate,
      arity: startLine.arity,
      hash: startLine.hash,
      instructions: instructions,
      rows: wroteRelationLine.rows,
      columns: wroteRelationLine.columns
    };
  }

  private async parseEvaluationIterations(): Promise<{ arity: number, hash: string, iterations: EvaluationIteration[] }> {
    const iterations: EvaluationIteration[] = [];
    let predicate: string = '';
    while (true) {
      const startLine = <StartPredicateEvaluationLine>await this.lineBuffer.expect(ProfileLineKind.START_PREDICATE_EVALUATION);
      if (iterations.length === 0) {
        predicate = startLine.predicate;
      }
      else if (startLine.predicate !== predicate) {
        reportError(startLine.lineNumber, `Mismatched predicate: expected '${predicate}', but got '${startLine.predicate}'.`);
      }

      if (startLine.iteration !== (iterations.length + 1)) {
        reportError(startLine.lineNumber, `Unexpected iteration number: expected '${iterations.length + 1}', but got '${startLine.iteration}'.`);
      }

      const tupleCountLine = <TupleCountsLine>await this.lineBuffer.expect(ProfileLineKind.TUPLE_COUNTS);
      if (tupleCountLine.predicate !== startLine.predicate) {
        reportError(tupleCountLine.lineNumber, `Mismatched predicate: expected '${startLine.predicate}', but got '${tupleCountLine.predicate}'.`);
      }

      const instructions = await this.parseRAInstructions();

      const deltaRowCountLine = <DeltaRowCountLine>await this.lineBuffer.expect(ProfileLineKind.DELTA_ROW_COUNT);

      iterations.push({
        index: iterations.length,
        instructions: instructions,
        rows: deltaRowCountLine.rows,
        order: deltaRowCountLine.order
      });

      const nextLine = await this.lineBuffer.peek();
      if (nextLine.kind !== ProfileLineKind.START_PREDICATE_EVALUATION) {
        return {
          arity: startLine.arity,
          hash: startLine.hash,
          iterations: iterations
        };
      }
    }
  }

  private async parseRecursivePredicateEvaluation(): Promise<RecursivePredicateEvaluationEvent> {
    const iterations = await this.parseEvaluationIterations();

    await this.lineBuffer.expect(ProfileLineKind.ACCUMULATING_DELTAS);

    const wroteRelationLine = <WroteRelationLine>await this.lineBuffer.expect(ProfileLineKind.WROTE_RELATION);

    return {
      kind: ProfileEventKind.RECURSIVE_PREDICATE_EVALUATION,
      predicate: wroteRelationLine.predicate,
      ...iterations
    };
  }

  private async parseRAInstructions(): Promise<RAInstruction[]> {
    const instructions: RAInstruction[] = [];
    while (true) {
      const line = await this.lineBuffer.peek();
      if (line.kind !== ProfileLineKind.RA_INSTRUCTION) {
        return instructions;
      }

      await this.lineBuffer.read();

      instructions.push({
        operation: line.operation,
        tupleCount: line.tupleCount,
        duplication: line.duplication
      });
    }
  }
}

async function main(): Promise<void> {
    const argv = yargs
    .command('profile', 'Collect profiling data', {

    })
    .option('log', {
      description: 'Log file to parse',
      type: 'string',
      demandOption: true
    })
    .help()
    .alias('help', 'h')
    .argv;

  console.log('Logging...');

  console.log(`--log ${argv.log}`);

  const inputStream = fs.createReadStream(argv.log, { encoding: 'utf8' });
  const lineBuffer = new LineBuffer(inputStream);
  const parser = new Parser(lineBuffer);
  while (true) {
    const event = await parser.parseEvent();
    if (event === undefined) {
      break;
    }
    console.log(event);
  }
}

(async () => { main(); })();
