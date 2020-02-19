import * as fs from 'fs-extra'
import * as yaml from 'js-yaml'
import * as yargs from 'yargs'
import { Parser, ProfileEventKind } from './parser';

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

  const inputStream = fs.createReadStream(argv.log, { encoding: 'utf8' });
  const parser = new Parser(inputStream);
  const evaluation = await parser.parseEvaluation();

  const stages = evaluation.stages.map(stage => {
    return {
      stage: stage.stage,
      stageKind: stage.stageKind,
      results: stage.results,
      time: stage.time,
      cumulativeTime: stage.cumulativeTime,
      events: stage.events.map(event => {
        switch (event.kind) {
          case ProfileEventKind.DATABASE_TABLE_EVALUATION:
            return {
              duration: event.duration,
              predicate: event.predicate,
              rows: event.rows,
              columns: event.columns
            };

          case ProfileEventKind.PREDICATE_EVALUATION:
            return {
              duration: event.duration,
              predicate: event.predicate,
              rows: event.rows,
              columns: event.columns
            };

          case ProfileEventKind.SCC_EVALUATION:
            {
              const iterations = event.iterations.map((iteration) => {
                return  iteration.predicates.map((predicate, predicateIndex) => {
                  return {
                    predicate: event.predicates[predicateIndex].currentDeltaPredicate,
                    rows: predicate.rows,
                    columns: event.predicates[predicateIndex].arity
                  };
                });
              });

              return {
                duration: event.duration,
                predicates: event.predicates.map(predicate => {
                  return {
                    predicate: predicate.predicate,
                    rows: predicate.rows,
                    columns: predicate.arity
                  };
                }),
                iterations: iterations.length
              };
            }

          default:
            throw new Error('Unexpected.');
        }
      })
    }
  });
  const allEvents: any[] = [];
  for (const stage of stages) {
    allEvents.push(...stage.events.map(event => {
      return {
        stage: stage.stage,
        ...event
      };
    }));
  }

  const sortedEvents = [...allEvents].sort((a, b) => {
    if (a.duration === b.duration) {
      return 0;
    }
    else if (a.duration > b.duration) {
      return -1;
    }
    else {
      return 1;
    }
  });

  const report = {
    stages: stages,
    topTen: sortedEvents.slice(0, 10)
  };

  const text = yaml.safeDump(report, {
    noRefs: true
  });
  console.log(text)
}

(async () => { main(); })();
