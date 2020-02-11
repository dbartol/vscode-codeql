import { reportError } from "./diagnostics";
import { splitStreamAtSeparators, lineEndings } from "./split-buffer";
import { Readable } from "stream";

export enum ProfileLineKind {
  END_OF_FILE = 'end_of_file',
  START_HOP_EVALUATION = 'start_hop_evaluation',
  START_PREDICATE_EVALUATION = 'start_predicate_evaluation',
  START_RECURSIVE_PREDICATE_EVALUATION = 'start_recursive_predicate_evaluation',
  INFERRED_EMPTY_RELATION = 'inferred_empty_relation',
  TUPLE_COUNTS = 'tuple_counts',
  RA_INSTRUCTION = 'ra_instruction',
  WROTE_RELATION = 'wrote_relation',
  HOP_RELATION = 'hop_relation',
  DELTA_ROW_COUNT = 'delta_row_count',
  EMPTY_DELTA = 'empty_delta',
  ACCUMULATING_DELTAS = 'accumulating_deltas'
}

export interface ProfileLineBase {
  lineNumber: number;
}

export interface TimestampedLine extends ProfileLineBase {
  elapsedSeconds: number;
}

export interface EndOfFileLine extends ProfileLineBase {
  kind: ProfileLineKind.END_OF_FILE;
}

export interface StartHopEvaluationLine extends TimestampedLine {
  kind: ProfileLineKind.START_HOP_EVALUATION;
  predicate: string;
  arity: number;
  hash: string;
  hop: string;
}

export interface StartPredicateEvaluationLine extends TimestampedLine {
  kind: ProfileLineKind.START_PREDICATE_EVALUATION;
  predicate: string;
  arity: number;
  hash: string;
}

export interface StartRecursivePredicateEvaluationLine extends TimestampedLine {
  kind: ProfileLineKind.START_RECURSIVE_PREDICATE_EVALUATION;
  predicate: string;
  arity: number;
  iteration: number;
  hash: string;
}

export interface InferredEmptyRelationLine extends TimestampedLine {
  kind: ProfileLineKind.INFERRED_EMPTY_RELATION;
  predicate: string;
  arity: number;
}

export interface TupleCountsLine extends ProfileLineBase {
  kind: ProfileLineKind.TUPLE_COUNTS;
  predicate: string;
}

export interface RAInstructionLine extends ProfileLineBase {
  kind: ProfileLineKind.RA_INSTRUCTION;
  operation: string;
  tupleCount?: number;
  duplication?: number;
}

export interface WroteRelationLine extends TimestampedLine {
  kind: ProfileLineKind.WROTE_RELATION;
  predicate: string;
  rows: number;
  columns: number;
}

export interface HopRelationLine extends TimestampedLine {
  kind: ProfileLineKind.HOP_RELATION;
  predicate: string;
  rows: number;
}

export interface DeltaRowCountLine extends ProfileLineBase {
  kind: ProfileLineKind.DELTA_ROW_COUNT;
  predicate: string;
  order: string;
  rows: number;
}

export interface EmptyDeltaLine extends TimestampedLine {
  kind: ProfileLineKind.EMPTY_DELTA;
  predicate: string;
  order: string;
}

export interface AccumulatingDeltasLine extends ProfileLineBase {
  kind: ProfileLineKind.ACCUMULATING_DELTAS;
}

export type ProfileLine = EndOfFileLine | StartPredicateEvaluationLine | StartRecursivePredicateEvaluationLine | StartHopEvaluationLine | InferredEmptyRelationLine | TupleCountsLine | RAInstructionLine | WroteRelationLine | HopRelationLine |
  DeltaRowCountLine | EmptyDeltaLine | AccumulatingDeltasLine;

const timestampRegex = /\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] \((\d+)s\)/;

function timestampedLineRegex(_pattern: RegExp): RegExp {
  return new RegExp(`^(?:${timestampRegex.source}) (?:${_pattern.source})`);
}

const startEvaluatingPredicateLineRegex = timestampedLineRegex(
  /Starting to evaluate predicate ([^/]+)\/(\d+)@([A-Fa-f0-9]+)$/
);
const startEvaluatingRecursivePredicateLineRegex = timestampedLineRegex(
  /Starting to evaluate predicate ([^/]+)\/(\d+)\[(\d+)\]@([A-Fa-f0-9]+) \(iteration \d+\)$/
);
const startEvaluatingHopLineRegex = timestampedLineRegex(
  /Starting to evaluate predicate ([^/]+)\/(\d+)\[(\d+)\]@([A-Fa-f0-9]+) = HOP (.+)$/
);
const tupleCountsLineRegex = timestampedLineRegex(/Tuple counts for (.*)+:/);
const nonReturnLineRegex = /^ +(\d+) +~(\d+)% +(.*)$/;
const returnLineRegex = /^ +(return .*)$/;
const wroteRelationLineRegex = timestampedLineRegex(/ >>> Wrote relation ([^ ]+) with (\d+) rows and (\d+) columns\.$/);
const hopRelationLineRegex = timestampedLineRegex(/ >>> Relation ([^ :]+): (\d+) rows using (\d+) MB$/);
const deltaRowCountLineRegex = timestampedLineRegex(/\s+- ([^ ]+) has (\d+) rows \(order for disjuncts: delta=(\d+|\<standard\>)\)\./);
const emptyDeltaLineRegex = timestampedLineRegex(/Empty delta for ([^ ]+) \(order for disjuncts: delta=(\d+|<standard>)\)\./);
const accumulatingDeltasLineRegex = timestampedLineRegex(/Accumulating deltas/);
const inferredEmptyRelationLineRegex = timestampedLineRegex(/Inferred that non-rec relation ([^/ ]+)\/(\d+) is empty$/);

export class LineBuffer {
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
        if (match = line.match(startEvaluatingRecursivePredicateLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.START_RECURSIVE_PREDICATE_EVALUATION,
            lineNumber: this.lineNumber,
            elapsedSeconds: parseInt(match[1]),
            predicate: match[2],
            arity: parseInt(match[3]),
            iteration: parseInt(match[4]),
            hash: match[5]
          };
        }
        else if (match = line.match(inferredEmptyRelationLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.INFERRED_EMPTY_RELATION,
            lineNumber: this.lineNumber,
            elapsedSeconds: parseInt(match[1]),
            predicate: match[2],
            arity: parseInt(match[3])
          };
        }
        else if (match = line.match(startEvaluatingHopLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.START_HOP_EVALUATION,
            lineNumber: this.lineNumber,
            elapsedSeconds: parseInt(match[1]),
            predicate: match[2],
            arity: parseInt(match[3]),
            hash: match[4],
            hop: match[5]
          }
        }
        else if (match = line.match(startEvaluatingPredicateLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.START_PREDICATE_EVALUATION,
            lineNumber: this.lineNumber,
            elapsedSeconds: parseInt(match[1]),
            predicate: match[2],
            arity: parseInt(match[3]),
            hash: match[4]
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
            elapsedSeconds: parseInt(match[1]),
            predicate: match[2],
            rows: parseInt(match[3]),
            columns: parseInt(match[4])
          };
        }
        else if (match = line.match(hopRelationLineRegex)) {
          this.nextLine = {
            kind: ProfileLineKind.HOP_RELATION,
            lineNumber: this.lineNumber,
            elapsedSeconds: parseInt(match[1]),
            predicate: match[2],
            rows: parseInt(match[3])
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
            kind: ProfileLineKind.EMPTY_DELTA,
            lineNumber: this.lineNumber,
            elapsedSeconds: parseInt(match[1]),
            predicate: match[2],
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

  public async expect(...kinds: ProfileLineKind[]): Promise<ProfileLine> {
    const line = await this.read();
    if (kinds.indexOf(line.kind) >= 0) {
      return line;
    }
    else {
      reportError(line.lineNumber, `Expected one of the following tokens: ${kinds.map(kind => `'${kind}'`).join(', ')}`);
    }
  }
}
