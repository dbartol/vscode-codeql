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

  console.log('Logging...');

  console.log(`--log ${argv.log}`);

  const inputStream = fs.createReadStream(argv.log, { encoding: 'utf8' });
  const parser = new Parser(inputStream);
  const events: any[] = [];
  while (true) {
    const event = await parser.parseEvent();
    if (event === undefined) {
      break;
    }
    switch (event.kind) {
      case ProfileEventKind.DATABASE_TABLE_EVALUATION:
        events.push({
          duration: event.duration,
          predicate: event.predicate,
          rows: event.rows
        });
        break;

      case ProfileEventKind.PREDICATE_EVALUATION:
        events.push({
          duration: event.duration,
          predicate: event.predicate,
          rows: event.rows
        });
        break;

      case ProfileEventKind.SCC_EVALUATION:
        {
          const iterations = event.iterations.map((iteration) => {
            return  iteration.predicates.map((predicate, predicateIndex) => {
              return {
                predicate: event.predicates[predicateIndex].currentDeltaPredicate,
                rows: predicate.rows
              };
            });
          });

          events.push({
            duration: event.duration,
            predicates: event.predicates.map(predicate => predicate.predicate),
            iterations: iterations.length
          });
        }
        break;
    }
  }

  const sortedEvents = [...events].sort((a, b) => {
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
    events: events,
    topTen: sortedEvents.slice(0, 10)
  };

  const text = yaml.safeDump(report, {
    noRefs: true
  });
  console.log(text)
}

(async () => { main(); })();
