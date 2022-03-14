import * as _ from 'lodash';
import moment from 'moment';
import clarinet from 'clarinet';

import {
  BufferLocation, EstimateDirection, NodeProp,
} from '@/enums';
import { IPlan } from '@/iplan';
import Node from '@/inode';

import { TextParser } from './parsers/text';

export class PlanService {

  private static instance: PlanService;
  private nodeId: number = 0;
  private textParser: TextParser = new TextParser();

  public createPlan(planName: string, planContent: any, planQuery: string): IPlan {
    // remove any extra white spaces in the middle of query
    // (\S) start match after any non-whitespace character => group 1
    // (?!$) don't start match after end of line
    // (\s{2,}) group of 2 or more white spaces
    // '$1 ' reuse group 1 and and a single space
    planQuery = planQuery.replace(/(\S)(?!$)(\s{2,})/gm, '$1 ');

    const plan: IPlan = {
      id: NodeProp.PLAN_TAG + new Date().getTime().toString(),
      name: planName || 'plan created on ' + moment().format('LLL'),
      createdOn: new Date(),
      content: planContent,
      query: planQuery,
      planStats: {},
      ctes: [],
      isAnalyze: _.has(planContent.Plan, NodeProp.ACTUAL_ROWS),
      isVerbose: this.findOutputProperty(planContent.Plan),
    };

    this.nodeId = 1;
    this.processNode(plan.content.Plan, plan);
    this.calculateMaximums(plan.content);
    return plan;
  }

  public isCTE(node: any) {
    return node[NodeProp.PARENT_RELATIONSHIP] === 'InitPlan' &&
      _.startsWith(node[NodeProp.SUBPLAN_NAME], 'CTE');
  }

  // Recursively walk down from the plan root node to compute various metrics
  public processNode(node: any, plan: any) {
    node.nodeId = this.nodeId++;
    this.calculatePlannerEstimate(node);

    _.each(node[NodeProp.PLANS], (child) => {
      // Disseminate workers planned info to parallel nodes (ie. Gather children)
      if (!this.isCTE(child) &&
        child[NodeProp.PARENT_RELATIONSHIP] !== 'InitPlan' &&
        child[NodeProp.PARENT_RELATIONSHIP] !== 'SubPlan') {
        child[NodeProp.WORKERS_PLANNED_BY_GATHER] = node[NodeProp.WORKERS_PLANNED] ||
          node[NodeProp.WORKERS_PLANNED_BY_GATHER];
      }
      if (this.isCTE(child)) {
        plan.ctes.push(child);
      }
      this.processNode(child, plan);
    });

    // Remove cte child plans.
    _.remove(node[NodeProp.PLANS], (child: any) => this.isCTE(child));

    // calculate actuals after processing child nodes so that actual duration
    // takes loops into account
    this.calculateActuals(node);
    this.calculateExclusives(node);
  }

  public calculateMaximums(content: any) {
    function recurse(nodes: any[]): any[] {
      return _.map(nodes, (node) => [node, recurse(node[NodeProp.PLANS])]);
    }

    const flat = _.flattenDeep(recurse([content.Plan as IPlan]));

    // Max Rows
    const largest = _.maxBy(flat, NodeProp.ACTUAL_ROWS);
    if (largest) {
      content.maxRows = largest[NodeProp.ACTUAL_ROWS];
    }

    // Max Cost
    const costliest = _.maxBy(flat, NodeProp.EXCLUSIVE_COST);
    if (costliest) {
      content.maxCost = costliest[NodeProp.EXCLUSIVE_COST];
    }

    // Max total cost
    const totalCostliest = _.maxBy(flat, NodeProp.TOTAL_COST);
    if (totalCostliest) {
      content.maxTotalCost = totalCostliest[NodeProp.TOTAL_COST];
    }

    // Slow
    const slowest = _.maxBy(flat, NodeProp.EXCLUSIVE_DURATION);
    if (slowest) {
      content.maxDuration = slowest[NodeProp.EXCLUSIVE_DURATION];
    }

    if (!content.maxBlocks) {
      content.maxBlocks = {};
    }

    function sumShared(o: Node) {
      return o[NodeProp.EXCLUSIVE_SHARED_HIT_BLOCKS] +
        o[NodeProp.EXCLUSIVE_SHARED_READ_BLOCKS] +
        o[NodeProp.EXCLUSIVE_SHARED_DIRTIED_BLOCKS] +
        o[NodeProp.EXCLUSIVE_SHARED_WRITTEN_BLOCKS];
    }
    const highestShared = _.maxBy(flat, (o) => {
      return sumShared(o);
    });
    if (highestShared && sumShared(highestShared)) {
      content.maxBlocks[BufferLocation.shared] = sumShared(highestShared);
    }

    function sumTemp(o: Node) {
      return o[NodeProp.EXCLUSIVE_TEMP_READ_BLOCKS] +
        o[NodeProp.EXCLUSIVE_TEMP_WRITTEN_BLOCKS];
    }
    const highestTemp = _.maxBy(flat, (o) => {
      return sumTemp(o);
    });
    if (highestTemp && sumTemp(highestTemp)) {
      content.maxBlocks[BufferLocation.temp] = sumTemp(highestTemp);
    }

    function sumLocal(o: Node) {
      return o[NodeProp.EXCLUSIVE_LOCAL_HIT_BLOCKS] +
        o[NodeProp.EXCLUSIVE_LOCAL_READ_BLOCKS] +
        o[NodeProp.EXCLUSIVE_LOCAL_DIRTIED_BLOCKS] +
        o[NodeProp.EXCLUSIVE_LOCAL_WRITTEN_BLOCKS];
    }
    const highestLocal = _.maxBy(flat, (o) => {
      return sumLocal(o);
    });
    if (highestLocal && sumLocal(highestLocal)) {
      content.maxBlocks[BufferLocation.local] = sumLocal(highestLocal);
    }
  }

  // actual duration and actual cost are calculated by subtracting child values from the total
  public calculateActuals(node: any) {
    if (!_.isUndefined(node[NodeProp.ACTUAL_TOTAL_TIME])) {
      // since time is reported for an invidual loop, actual duration must be adjusted by number of loops
      // number of workers is also taken into account
      const workers = (node[NodeProp.WORKERS_PLANNED_BY_GATHER] || 0) + 1;
      node[NodeProp.ACTUAL_TOTAL_TIME] = node[NodeProp.ACTUAL_TOTAL_TIME] * node[NodeProp.ACTUAL_LOOPS] / workers;
      node[NodeProp.ACTUAL_STARTUP_TIME] = node[NodeProp.ACTUAL_STARTUP_TIME] * node[NodeProp.ACTUAL_LOOPS] / workers;
      node[NodeProp.EXCLUSIVE_DURATION] = node[NodeProp.ACTUAL_TOTAL_TIME];

      const duration = node[NodeProp.EXCLUSIVE_DURATION] - this.childrenDuration(node, 0);
      node[NodeProp.EXCLUSIVE_DURATION] = duration > 0 ? duration : 0;
    }

    if (node[NodeProp.TOTAL_COST]) {
      node[NodeProp.EXCLUSIVE_COST] = node[NodeProp.TOTAL_COST];
    }

    // Exclusive cost of node should be subtract total cost of child plans.
    _.each(node[NodeProp.PLANS], (subPlan) => {
      if (subPlan[NodeProp.PARENT_RELATIONSHIP] !== 'InitPlan' && subPlan[NodeProp.TOTAL_COST]) {
        node[NodeProp.EXCLUSIVE_COST] = node[NodeProp.EXCLUSIVE_COST] - subPlan[NodeProp.TOTAL_COST];
      }
    });

    if (node[NodeProp.EXCLUSIVE_COST] < 0) {
      node[NodeProp.EXCLUSIVE_COST] = 0;
    }

    _.each(
      ['ACTUAL_ROWS', 'PLAN_ROWS', 'ROWS_REMOVED_BY_FILTER', 'ROWS_REMOVED_BY_JOIN_FILTER'],
      (prop: keyof typeof NodeProp) => {
        if (!_.isUndefined(node[NodeProp[prop]])) {
          const revisedProp = prop + '_REVISED' as keyof typeof NodeProp;
          const loops = node[NodeProp.ACTUAL_LOOPS] || 1;
          node[NodeProp[revisedProp]] = node[NodeProp[prop]] * loops;
        }
      },
    );
  }

  // recursive function to get the sum of actual durations of a a node children
  public childrenDuration(node: Node, duration: number) {
    _.each(node[NodeProp.PLANS], (child) => {
      // Subtract sub plans duration from this node except for InitPlans
      // (ie. CTE)
      if (child[NodeProp.PARENT_RELATIONSHIP] !== 'InitPlan') {
        duration += child[NodeProp.EXCLUSIVE_DURATION] || 0; // Duration may not be set
        duration = this.childrenDuration(child, duration);
      }
    });
    return duration;
  }

  // figure out order of magnitude by which the planner mis-estimated how many rows would be
  // invloved in this node
  public calculatePlannerEstimate(node: any) {
    if (node[NodeProp.ACTUAL_ROWS] === undefined) {
      return;
    }
    node[NodeProp.PLANNER_ESTIMATE_FACTOR] = node[NodeProp.ACTUAL_ROWS] / node[NodeProp.PLAN_ROWS];
    node[NodeProp.PLANNER_ESTIMATE_DIRECTION] = EstimateDirection.none;

    if (node[NodeProp.ACTUAL_ROWS] > node[NodeProp.PLAN_ROWS]) {
      node[NodeProp.PLANNER_ESTIMATE_DIRECTION] = EstimateDirection.under;
    }
    if (node[NodeProp.ACTUAL_ROWS] < node[NodeProp.PLAN_ROWS]) {
      node[NodeProp.PLANNER_ESTIMATE_DIRECTION] = EstimateDirection.over;
      node[NodeProp.PLANNER_ESTIMATE_FACTOR] = node[NodeProp.PLAN_ROWS] / node[NodeProp.ACTUAL_ROWS];
    }
  }

  public cleanupSource(source: string) {
    // Remove frames around, handles |, ║,
    source = source.replace(/^(\||║|│)(.*)\1\r?\n/gm, '$2\n');

    // Remove separator lines from various types of borders
    source = source.replace(/^\+-+\+\r?\n/gm, '');
    source = source.replace(/^(-|─|═)\1+\r?\n/gm, '');
    source = source.replace(/^(├|╟|╠|╞)(─|═)\2*(┤|╢|╣|╡)\r?\n/gm, '');

    // Remove more horizontal lines
    source = source.replace(/^\+-+\+\r?\n/gm, '');
    source = source.replace(/^└(─)+┘\r?\n/gm, '');
    source = source.replace(/^╚(═)+╝\r?\n/gm, '');
    source = source.replace(/^┌(─)+┐\r?\n/gm, '');
    source = source.replace(/^╔(═)+╗\r?\n/gm, '');

    // Remove quotes around lines, both ' and "
    source = source.replace(/^(["'])(.*)\1\r?\n/gm, '$2\n');

    // Remove "+" line continuations
    source = source.replace(/\s*\+\r?\n/g, '\n');

    // Remove "↵" line continuations
    source = source.replace(/↵\r?/gm, '\n');

    // Remove "query plan" header
    source = source.replace(/^\s*QUERY PLAN\s*\r?\n/m, '');

    // Remove rowcount
    // example: (8 rows)
    // Note: can be translated
    // example: (8 lignes)
    source = source.replace(/^\(\d+\s+[a-z]*s?\)(\r?\n|$)/gm, '\n');

    return source;
  }

  public fromSource(source: string) {
    source = this.cleanupSource(source);

    let isJson = false;
    try {
      isJson = JSON.parse(source);
    } catch (error) {
      // continue
    }

    if (isJson) {
      return this.parseJson(source);
    } else if (/^(\s*)(\[|\{)\s*\n.*?\1(\]|\})\s*/gms.exec(source)) {
      return this.fromJson(source);
    }

    return this.textParser.fromText(source);
  }

  public fromJson(source: string) {
    // We need to remove things before and/or after explain
    // To do this, first - split explain into lines...
    const sourceLines = source.split(/[\r\n]+/);

    // Now, find first line of explain, and cache it's prefix (some spaces ...)
    let prefix = '';
    let firstLineIndex = 0;
    _.each(sourceLines, (l: string, index: number) => {
      const matches = /^(\s*)(\[|\{)\s*$/.exec(l);
      if (matches) {
        prefix = matches[1];
        firstLineIndex = index;
        return false;
      }
    });
    // now find last line
    let lastLineIndex = 0;
    _.each(sourceLines, (l: string, index: number) => {
      const matches = new RegExp('^' + prefix + '(\]|\})\s*$').exec(l);
      if (matches) {
        lastLineIndex = index;
        return false;
      }
    });

    const useSource: string = sourceLines.slice(firstLineIndex, lastLineIndex + 1).join('\n');

    return this.parseJson(useSource);
  }

  // Just for backward compatiblity.
  public fromText(source: string) {
    return this.textParser.fromText(source);
  }

  // Stream parse JSON as it can contain duplicate keys (workers)
  public parseJson(source: string) {
    const parser = clarinet.parser();
    const elements: any[] = [];
    let root: any = null;
    // Store the level and duplicated object|array
    let duplicated: [number, any] | null = null;
    parser.onvalue = (v: any) => {
      const current = elements[elements.length - 1];
      if (_.isArray(current)) {
        current.push(v);
      } else {
        const keys = Object.keys(current);
        const lastKey = keys[keys.length - 1];
        current[lastKey] = v;
      }
    };
    parser.onopenobject = (key: any) => {
      const o: { [key: string]: any } = {};
      o[key] = null;
      elements.push(o);
    };
    parser.onkey = (key: any) => {
      const current = elements[elements.length - 1];
      const keys = Object.keys(current);
      if (keys.indexOf(key) !== -1) {
        duplicated = [elements.length - 1, current[key]];
      } else {
        current[key] = null;
      }
    };
    parser.onopenarray = () => {
      elements.push([]);
    };
    parser.oncloseobject = parser.onclosearray = () => {
      const popped = elements.pop();

      if (!elements.length) {
        root = popped;
      } else {
        const current = elements[elements.length - 1];

        if (duplicated && duplicated[0] === elements.length - 1) {
          _.merge(duplicated[1], popped);
          duplicated = null;
        } else {
          if (_.isArray(current)) {
            current.push(popped);
          } else {
            const keys = Object.keys(current);
            const lastKey = keys[keys.length - 1];
            current[lastKey] = popped;
          }
        }
      }
    };
    parser.write(source).close();
    if (root instanceof Array) {
      root = root[0];
    }
    return root;
  }

  private calculateExclusives(node: Node) {
    // Caculate inclusive value for the current node for the given property
    const properties: Array<keyof typeof NodeProp> = [
      'SHARED_HIT_BLOCKS',
      'SHARED_READ_BLOCKS',
      'SHARED_DIRTIED_BLOCKS',
      'SHARED_WRITTEN_BLOCKS',
      'TEMP_READ_BLOCKS',
      'TEMP_WRITTEN_BLOCKS',
      'LOCAL_HIT_BLOCKS',
      'LOCAL_READ_BLOCKS',
      'LOCAL_DIRTIED_BLOCKS',
      'LOCAL_WRITTEN_BLOCKS',
      'IO_READ_TIME',
      'IO_WRITE_TIME',
    ];
    _.each(properties, (property) => {
      const sum = _.sumBy(
        node[NodeProp.PLANS],
        (child: Node) => {

          return child[NodeProp[property]] || 0;
        },
      );
      const exclusivePropertyString = 'EXCLUSIVE_' + property as keyof typeof NodeProp;
      node[NodeProp[exclusivePropertyString]] = node[NodeProp[property]] - sum;
    });
  }

  private findOutputProperty(node: Node): boolean {
    // resursively look for an "Output" property
    const children = node.Plans;
    if (!children) {
      return false;
    }
    return _.some(children, (child) => {
      return _.has(child, NodeProp.OUTPUT) || this.findOutputProperty(child);
    });
  }
}
