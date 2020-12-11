import { DebugAdapterDescriptor, DebugAdapterDescriptorFactory, DebugAdapterExecutable, DebugAdapterInlineImplementation, DebugSession, Disposable, ProviderResult, Uri } from 'vscode';
import { InitializedEvent, LoggingDebugSession, StoppedEvent } from 'vscode-debugadapter';
import { DebugProtocol } from 'vscode-debugprotocol';
import { QueryRunnerUI } from './query-runner-ui';
import { DisposableObject } from './vscode-utils/disposable-object';
import * as messages from './pure/messages';
import * as path from 'path';
import { CancellationTokenSource } from 'vscode-jsonrpc';
import { determineSelectedQuery, QueryWithResults, SelectedQuery } from './run-queries';

interface CodeQLLaunchRequestArguments extends DebugProtocol.LaunchRequestArguments {
  query: string;
}

interface StackEntry {
  frame: DebugProtocol.StackFrame;
  entry: messages.DebugStackEntry;
}

class QLDebugSession extends LoggingDebugSession implements Disposable {
  private entryPoint: SelectedQuery | undefined = undefined;
  private tokenSource: CancellationTokenSource | undefined = undefined;
  private quickeval: Promise<QueryWithResults | undefined> | undefined = undefined;
  private stack: StackEntry[] = [];
  private debugOperations: messages.DebugOperation[] = [];

  constructor(private readonly queryRunnerUI: QueryRunnerUI) {
    super();
  }

  public dispose(): void {
    this.cancelEvaluation();
  }

  protected dispatchRequest(request: DebugProtocol.Request): void {
    super.dispatchRequest(request);
  }

  protected initializeRequest(response: DebugProtocol.InitializeResponse, _args: DebugProtocol.InitializeRequestArguments): void {
    response.body = response.body ?? {};
    response.body.supportsStepBack = true;
    response.body.supportsStepInTargetsRequest = true;

    this.sendResponse(response);

    this.sendEvent(new InitializedEvent());
  }

  protected configurationDoneRequest(response: DebugProtocol.ConfigurationDoneResponse, args: DebugProtocol.ConfigurationDoneArguments, request?: DebugProtocol.Request): void {
    super.configurationDoneRequest(response, args, request);
  }

  protected disconnectRequest(response: DebugProtocol.DisconnectResponse, _args: DebugProtocol.DisconnectArguments, _request?: DebugProtocol.Request): void {
    response.body = response.body ?? {};
    // Neither of the args (`terminateDebuggee` and `restart`) matter for CodeQL.

    this.sendResponse(response);
  }

  protected launchRequest(response: DebugProtocol.LaunchResponse, args: CodeQLLaunchRequestArguments, _request?: DebugProtocol.Request): void {
    this.launch(response, args);  //TODO: Cancelation?
  }

  protected threadsRequest(response: DebugProtocol.ThreadsResponse, request?: DebugProtocol.Request): void {
    response.body = response.body ?? {};
    response.body.threads = [
      {
        id: 1,
        name: 'Evaluation thread'
      }
    ];

    super.threadsRequest(response, request);
  }

  protected stackTraceRequest(response: DebugProtocol.StackTraceResponse, _args: DebugProtocol.StackTraceArguments, _request?: DebugProtocol.Request): void {
    response.body = response.body ?? {};
    // The QuickEval interface returns stack entries in the order in which they were stepped into.
    // This matches the order of QuickEval `DebugOperation`s, but is backwards from what the debug
    // adapter protocol expects.
    response.body.stackFrames = this.stack.map(entry => entry.frame).reverse();

    super.stackTraceRequest(response, _args, _request);
  }

  private async launch(response: DebugProtocol.LaunchResponse, args: CodeQLLaunchRequestArguments): Promise<void> {
    response.body = response.body ?? {};

    this.entryPoint = await determineSelectedQuery(Uri.file(path.resolve(args.query)), true);
    this.quickEval([], response, 'entry');
  }

  private navigateToFirstChild(): messages.DebugOperation[] | undefined {
    const currentEntry = this.stack[this.stack.length - 1].entry;
    if (currentEntry.node.children.length > 0) {
      return [
        ...this.debugOperations,
        { childIndex: 0 }
      ];
    } else {
      // No children.
      return undefined;
    }
  }

  private navigateToNext(): messages.DebugOperation[] | undefined {
    let stackIndex = this.stack.length - 1;
    while (stackIndex > 0) {
      const indexInParent = this.debugOperations[stackIndex - 1].childIndex;
      const parentEntry = this.stack[stackIndex - 1].entry;
      const nextSiblingIndex = indexInParent + 1;
      if (nextSiblingIndex < parentEntry.node.children.length) {
        return [
          ...this.debugOperations.slice(0, stackIndex - 1),
          { childIndex: nextSiblingIndex }
        ];
      }

      stackIndex--;
    }

    // We're at the root.
    return undefined;
  }

  protected nextRequest(response: DebugProtocol.NextResponse, _args: DebugProtocol.NextArguments, _request?: DebugProtocol.Request): void {
    this.quickEval(this.navigateToNext() ?? this.debugOperations, response, 'step');
  }

  protected stepInRequest(response: DebugProtocol.StepInResponse, _args: DebugProtocol.StepInArguments, _request?: DebugProtocol.Request): void {
    this.quickEval(this.navigateToFirstChild() ?? this.navigateToNext() ?? this.debugOperations, response, 'step');
  }

  private async quickEval(debugOperations: messages.DebugOperation[], response: DebugProtocol.Response, stopReason: string): Promise<void> {
    this.debugOperations = debugOperations;
    const selectedQuery = {
      ...this.entryPoint!,
      quickEvalDebugOperations: this.debugOperations
    };
    this.tokenSource = new CancellationTokenSource();
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    this.quickeval = this.queryRunnerUI.compileAndRunQuery(selectedQuery, _p => { }, this.tokenSource.token);
    // TODO: Handle exception from `compileAndRunQuery()`.
    // Send the response immediately after we start evaluating.
    this.sendResponse(response);

    const info = await this.quickeval;
    this.disposeTokenSource();
    this.quickeval = undefined;
    if (info !== undefined) {
      this.stack = info.compilationResult.debugStack!.map((entry, index) => {
        return {
          entry,
          frame: {
            id: index + 1,
            line: entry.node.position.line,
            column: entry.node.position.column,
            endLine: entry.node.position.endLine,
            endColumn: entry.node.position.endColumn,
            name: 'QuickEval Frame',
            source: {
              name: path.basename(entry.node.position.fileName),
              path: entry.node.position.fileName
            }
          }
        };
      });

      // Pretend that we've "stopped".
      const stoppedEvent = new StoppedEvent(stopReason, 1);
      this.sendEvent(stoppedEvent);
    }
  }

  private disposeTokenSource(): void {
    this.tokenSource!.dispose();
    this.tokenSource = undefined;
  }

  private cancelEvaluation(): void {
    if (this.tokenSource !== undefined) {
      this.tokenSource.cancel();
      this.disposeTokenSource();
    }
  }
}

export class QLDebugAdapterDescriptorFactory extends DisposableObject implements DebugAdapterDescriptorFactory {
  constructor(private readonly queryRunnerUI: QueryRunnerUI) {
    super();
  }

  public createDebugAdapterDescriptor(_session: DebugSession, _executable: DebugAdapterExecutable | undefined): ProviderResult<DebugAdapterDescriptor> {
    return new DebugAdapterInlineImplementation(new QLDebugSession(this.queryRunnerUI));
  }
}
