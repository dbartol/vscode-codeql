import { Readable } from 'stream';
import { reportError } from './diagnostics';
import { LineBuffer, ProfileLineKind, WroteRelationLine, StartPredicateEvaluationLine, TupleCountsLine, StartRecursivePredicateEvaluationLine, StartHopEvaluationLine, HopRelationLine, InferredEmptyRelationLine } from './line-buffer';

export interface RAInstruction {
  operation: string;
  tupleCount?: number;
  duplication?: number;
}

export enum ProfileEventKind {
  PREDICATE_EVALUATION = 'predicate_evaluation',
  SCC_EVALUATION = 'scc_evaluation',
  DATABASE_TABLE_EVALUATION = 'database_table_evaluation',
  ACCUMULATING_DELTAS = 'accumulating_deltas'
}

export interface RAOperation {
  instructions: RAInstruction[];
}

export interface HopOperation {
  hop: string;
}

export interface EmptyOperation {
};

export type Operation = RAOperation | HopOperation | EmptyOperation;

export interface EventBase {
  elapsedSeconds: number;
  duration: number;
}

export interface PredicateEvaluationEvent extends EventBase {
  kind: ProfileEventKind.PREDICATE_EVALUATION;
  predicate: string;
  arity: number;
  hash: string;
  operation: Operation;
  rows: number,
  columns: number
}

export interface RecursivePredicate {
  predicate: string;
  deltaPredicate: string;
  currentDeltaPredicate: string;
  arity: number;
  hash: string;
  rows: number;
}

export interface PredicateIteration {
  rows: number;
  order: string;
  operation: Operation;
}

export interface RecursiveIteration {
  predicates: PredicateIteration[];
}

export interface SccEvaluationEvent extends EventBase {
  kind: ProfileEventKind.SCC_EVALUATION;
  predicates: RecursivePredicate[];
  iterations: RecursiveIteration[];
}

export interface DatabaseTableEvaluationEvent extends EventBase {
  kind: ProfileEventKind.DATABASE_TABLE_EVALUATION;
  predicate: string;
  rows: number;
  columns: number;
}

export interface AccumulatingDeltasEvent {
  kind: ProfileEventKind.ACCUMULATING_DELTAS;
}

export type ProfileEvent = PredicateEvaluationEvent | SccEvaluationEvent | DatabaseTableEvaluationEvent | AccumulatingDeltasEvent;

interface RecursivePredicateEvaluation {
  lineNumber: number;
  startTime: number;
  iteration: number | undefined;
  currentDeltaPredicate: string;
  operation: Operation;
  deltaPredicate: string;
  rows: number;
  order: string;
  arity: number;
  hash: string;
}

export class Parser {
  private readonly lineBuffer: LineBuffer;

  constructor(inputStream: Readable) {
    this.lineBuffer = new LineBuffer(inputStream);
  }

  public async parseEvent(): Promise<ProfileEvent | undefined> {
    while (true) {
      const firstLine = await this.lineBuffer.peek();
      switch (firstLine.kind) {
        case ProfileLineKind.END_OF_FILE:
          await this.lineBuffer.read();
          return undefined;

        case ProfileLineKind.START_PREDICATE_EVALUATION: {
          const evaluation = await this.parsePredicateEvaluation();
          if (evaluation !== undefined) {
            return evaluation;
          }
        }
          break;

        case ProfileLineKind.START_HOP_EVALUATION:
          return await this.parseHopEvaluation();

        case ProfileLineKind.START_RECURSIVE_PREDICATE_EVALUATION:
          return await this.parseSccEvaluation();

        case ProfileLineKind.INFERRED_EMPTY_RELATION:
          return await this.parseEmptyRelation();

        case ProfileLineKind.WROTE_RELATION:
          return await this.parseDatabaseTableEvaluation();

        case ProfileLineKind.ACCUMULATING_DELTAS:
          return await this.parseAccumulatingDeltas();

        default:
          await this.lineBuffer.read();
          reportError(firstLine.lineNumber, `Unexpected line: '${firstLine.kind}'.`);
      }
    }
  }

  private async parseDatabaseTableEvaluation(): Promise<DatabaseTableEvaluationEvent> {
    const line = <WroteRelationLine>await this.lineBuffer.expect(ProfileLineKind.WROTE_RELATION);
    return {
      kind: ProfileEventKind.DATABASE_TABLE_EVALUATION,
      elapsedSeconds: line.elapsedSeconds,
      duration: 0,  //REVIEW
      predicate: line.predicate,
      rows: line.rows,
      columns: line.columns
    };
  }

  private async parseAccumulatingDeltas(): Promise<AccumulatingDeltasEvent> {
    await this.lineBuffer.expect(ProfileLineKind.ACCUMULATING_DELTAS);

    return {
      kind: ProfileEventKind.ACCUMULATING_DELTAS
    };
  }

  private async parsePredicateEvaluation(): Promise<PredicateEvaluationEvent | undefined> {
    const startLine = <StartPredicateEvaluationLine>await this.lineBuffer.expect(ProfileLineKind.START_PREDICATE_EVALUATION);

    const tupleCountLine = <TupleCountsLine>await this.lineBuffer.expect(ProfileLineKind.TUPLE_COUNTS);
    if (tupleCountLine.predicate !== startLine.predicate) {
      reportError(tupleCountLine.lineNumber, `Mismatched predicate: expected '${startLine.predicate}', but got '${tupleCountLine.predicate}'.`);
    }

    const instructions = await this.parseRAInstructions();

    const endLine = await this.lineBuffer.expect(ProfileLineKind.WROTE_RELATION, ProfileLineKind.INCOMPLETE_PREDICATE);
    switch (endLine.kind) {
      case ProfileLineKind.WROTE_RELATION:
        if (endLine.predicate !== startLine.predicate) {
          reportError(endLine.lineNumber, `Mismatched predicate: expected '${startLine.predicate}', but got '${endLine.predicate}'.`);
        }

        return {
          kind: ProfileEventKind.PREDICATE_EVALUATION,
          elapsedSeconds: startLine.elapsedSeconds,
          duration: endLine.elapsedSeconds - startLine.elapsedSeconds,
          predicate: startLine.predicate,
          arity: startLine.arity,
          hash: startLine.hash,
          operation: {
            instructions: instructions
          },
          rows: endLine.rows,
          columns: endLine.columns
        };

      case ProfileLineKind.INCOMPLETE_PREDICATE:
        return undefined;

      default:
        throw new Error('Unexpected.');
    }
  }

  private async parseEmptyRelation(): Promise<PredicateEvaluationEvent> {
    const startLine = <InferredEmptyRelationLine>await this.lineBuffer.expect(ProfileLineKind.INFERRED_EMPTY_RELATION);

    const wroteRelationLine = <WroteRelationLine>await this.lineBuffer.expect(ProfileLineKind.WROTE_RELATION);
    if (wroteRelationLine.predicate !== startLine.predicate) {
      reportError(wroteRelationLine.lineNumber, `Mismatched predicate: expected '${startLine.predicate}', but got '${wroteRelationLine.predicate}'.`);
    }

    return {
      kind: ProfileEventKind.PREDICATE_EVALUATION,
      elapsedSeconds: startLine.elapsedSeconds,
      duration: wroteRelationLine.elapsedSeconds - startLine.elapsedSeconds,
      predicate: startLine.predicate,
      arity: startLine.arity,
      hash: '',
      operation: {},
      rows: wroteRelationLine.rows,
      columns: wroteRelationLine.columns
    };
  }

  private async parseHopEvaluation(): Promise<PredicateEvaluationEvent> {
    const startLine = <StartHopEvaluationLine>await this.lineBuffer.expect(ProfileLineKind.START_HOP_EVALUATION);

    const hopRelationLine = <HopRelationLine>await this.lineBuffer.expect(ProfileLineKind.HOP_RELATION);
    if (hopRelationLine.predicate !== startLine.predicate) {
      reportError(hopRelationLine.lineNumber, `Mismatched predicate: expected '${startLine.predicate}', but got '${hopRelationLine.predicate}'.`);
    }

    return {
      kind: ProfileEventKind.PREDICATE_EVALUATION,
      elapsedSeconds: startLine.elapsedSeconds,
      duration: hopRelationLine.elapsedSeconds - startLine.elapsedSeconds,
      predicate: startLine.predicate,
      arity: startLine.arity,
      hash: startLine.hash,
      operation: {
        hop: startLine.hop
      },
      rows: hopRelationLine.rows,
      columns: startLine.arity
    };
  }

  private async parseRecursivePredicateEvaluations(): Promise<RecursivePredicateEvaluation[]> {
    const predicateEvaluations: RecursivePredicateEvaluation[] = [];
    while (true) {
      const line = await this.lineBuffer.peek();
      switch (line.kind) {
        case ProfileLineKind.START_RECURSIVE_PREDICATE_EVALUATION:
          predicateEvaluations.push(await this.parseRecursivePredicateEvaluation());
          break;

        case ProfileLineKind.EMPTY_DELTA: {
          await this.lineBuffer.read();
          // We can get an "Empty delta..." line without actually evaluating anything, but it should only happen on
          // iterations after the first.
          predicateEvaluations.push({
            arity: 0,
            currentDeltaPredicate: '',
            deltaPredicate: line.predicate,
            hash: '',
            operation: {},
            lineNumber: line.lineNumber,
            startTime: line.elapsedSeconds,
            order: line.order,
            rows: 0,
            iteration: undefined
          });
        }
          break;

        default:
          return predicateEvaluations;
      }
    }
  }

  private async parseSccEvaluation(): Promise<SccEvaluationEvent> {
    const evaluations = await this.parseRecursivePredicateEvaluations();

    let predicateCount = evaluations.findIndex(evaluation => evaluation.iteration !== 1);
    if (predicateCount < 0) {
      predicateCount = evaluations.length;
    }

    const predicates = evaluations.slice(0, predicateCount).map(evaluation => {
      return {
        currentDeltaPredicate: evaluation.currentDeltaPredicate,
        deltaPredicate: evaluation.deltaPredicate,
        arity: evaluation.arity,
        hash: evaluation.hash
      };
    });

    const iterations: RecursiveIteration[] = [];
    let iteration = 1;
    for (let firstEvaluation = 0; firstEvaluation < evaluations.length; firstEvaluation += predicateCount) {
      if ((firstEvaluation + predicateCount) > evaluations.length) {
        reportError(evaluations[evaluations.length - 1].lineNumber, `Not enough evaluations in iteration ${iteration}. Expected ${predicateCount}, but got ${evaluations.length - firstEvaluation}.`);
      }

      const iterationEvaluations = evaluations.slice(firstEvaluation, firstEvaluation + predicateCount);
      const predicateIterations = iterationEvaluations.map((evaluation, index) => {
        if ((evaluation.iteration !== undefined) && (evaluation.iteration !== iteration)) {
          reportError(evaluation.lineNumber, `Unexpected iteration index. Expected ${iteration}, but got ${evaluation.iteration}.`);
        }
        if (evaluation.deltaPredicate !== predicates[index].deltaPredicate) {
          reportError(evaluation.lineNumber, `Unexpected predicate name. Expected '${predicates[index].deltaPredicate}', but got '${evaluation.deltaPredicate}.`);
        }

        return {
          rows: evaluation.rows,
          order: evaluation.order,
          operation: evaluation.operation
        };
      });

      iterations.push({ predicates: predicateIterations });

      iteration++;
    }

    await this.lineBuffer.expect(ProfileLineKind.ACCUMULATING_DELTAS);

    let lastWriteTime = 0;
    const fullPredicates: RecursivePredicate[] = [];
    for (const predicate of predicates) {
      const wroteRelationLine = <WroteRelationLine>(await this.lineBuffer.expect(ProfileLineKind.WROTE_RELATION));
      lastWriteTime = Math.max(lastWriteTime, wroteRelationLine.elapsedSeconds);
      fullPredicates.push({
        ...predicate,
        rows: wroteRelationLine.rows,
        predicate: wroteRelationLine.predicate
      });
    };

    return {
      kind: ProfileEventKind.SCC_EVALUATION,
      elapsedSeconds: evaluations[0].startTime,
      duration: lastWriteTime - evaluations[0].startTime,
      predicates: fullPredicates,
      iterations: iterations
    };
  }

  private async parseRecursivePredicateEvaluation(): Promise<RecursivePredicateEvaluation> {
    const startLine = <StartRecursivePredicateEvaluationLine>await this.lineBuffer.expect(ProfileLineKind.START_RECURSIVE_PREDICATE_EVALUATION);

    const tupleCountLine = <TupleCountsLine>await this.lineBuffer.expect(ProfileLineKind.TUPLE_COUNTS);
    if (tupleCountLine.predicate !== startLine.predicate) {
      reportError(tupleCountLine.lineNumber, `Mismatched predicate: expected '${startLine.predicate}', but got '${tupleCountLine.predicate}'.`);
    }

    const instructions = await this.parseRAInstructions();

    const deltaLine = await this.lineBuffer.expect(ProfileLineKind.DELTA_ROW_COUNT, ProfileLineKind.EMPTY_DELTA);
    switch (deltaLine.kind) {
      case ProfileLineKind.DELTA_ROW_COUNT:
        return {
          lineNumber: startLine.lineNumber,
          startTime: startLine.elapsedSeconds,
          iteration: startLine.iteration,
          currentDeltaPredicate: startLine.predicate,
          deltaPredicate: deltaLine.predicate,
          operation: { instructions: instructions },
          rows: deltaLine.rows,
          order: deltaLine.order,
          arity: startLine.arity,
          hash: startLine.hash
        };

      case ProfileLineKind.EMPTY_DELTA:
        return {
          lineNumber: startLine.lineNumber,
          startTime: startLine.elapsedSeconds,
          iteration: startLine.iteration,
          currentDeltaPredicate: startLine.predicate,
          deltaPredicate: deltaLine.predicate,
          operation: { instructions: instructions },
          rows: 0,
          order: deltaLine.order,
          arity: startLine.arity,
          hash: startLine.hash
        };

      default:
        throw new Error('Unexpected.');
    }
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

