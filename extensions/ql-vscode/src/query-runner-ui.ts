import { CodeQLCliServer } from './cli';
import { DatabaseUI } from './databases-ui';
import * as helpers from './helpers';
import { InterfaceManager } from './interface';
import { QueryHistoryManager } from './query-history';
import { DisposableObject } from './vscode-utils/disposable-object';
import * as qsClient from './queryserver-client';
import { CompletedQuery } from './query-results';
import { WebviewReveal } from './interface-utils';
import { CancellationToken } from 'vscode';
import { compileAndRunQueryAgainstDatabase, QueryWithResults, SelectedQuery } from './run-queries';

export class QueryRunnerUI extends DisposableObject {
  constructor(private readonly intm: InterfaceManager, private readonly qhm: QueryHistoryManager,
    private readonly databaseUI: DatabaseUI, private readonly cliServer: CodeQLCliServer,
    private readonly qs: qsClient.QueryServerClient) {
    super();
  }

  async showResultsForCompletedQuery(
    query: CompletedQuery,
    forceReveal: WebviewReveal
  ): Promise<void> {
    await this.intm.showResults(query, forceReveal, false);
  }

  async compileAndRunQuery(
    selectedQuery: SelectedQuery,
    progress: helpers.ProgressCallback,
    token: CancellationToken,
  ): Promise<QueryWithResults | undefined> {
    if (this.qs !== undefined) {
      const dbItem = await this.databaseUI.getDatabaseItem(progress, token);
      if (dbItem === undefined) {
        throw new Error('Can\'t run query without a selected database');
      }
      const info = await compileAndRunQueryAgainstDatabase(
        this.cliServer,
        this.qs,
        dbItem,
        selectedQuery,
        progress,
        token
      );
      const item = this.qhm.addQuery(info);
      await this.showResultsForCompletedQuery(item, WebviewReveal.NotForced);
      // The call to showResults potentially creates SARIF file;
      // Update the tree item context value to allow viewing that
      // SARIF file from context menu.
      await this.qhm.updateTreeItemContextValue(item);

      return info;
    } else {
      return undefined;
    }
  }
}
