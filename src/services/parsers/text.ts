/**
 * A parser to parse text-format plan data.
 */

import * as _ from 'lodash';

import {
    SortGroupsProp, NodeProp, SortSpaceMemory, WorkerProp,
} from '@/enums';
import Node from '@/inode';
import Worker from '@/iworker';

import { splitBalanced } from '@/services/help-service';

export class TextParser {
    public splitIntoLines(text: string): string[] {
        // Splits source into lines, while fixing (well, trying to fix)
        // cases where input has been force-wrapped to some length.
        const out: string[] = [];
        const lines = text.split(/\r?\n/);
        const countChar = (str: string, ch: RegExp) => (str.match(ch) || []).length;

        _.each(lines, (line: string) => {
            if (countChar(line, /\)/g) > countChar(line, /\(/g)) {
                // if there more closing parenthesis this means that it's the
                // continuation of a previous line
                out[out.length - 1] += line;
            } else if (line.match(/^(?:Total\s+runtime|Planning\s+time|Execution\s+time|Time|Filter|Output|JIT)/i)) {
                out.push(line);
            } else if (
                line.match(/^\S/) || // doesn't start with a blank space (allowed only for the first node)
                line.match(/^\s*\(/) // first non-blank character is an opening parenthesis
            ) {
                if (0 < out.length) {
                    out[out.length - 1] += line;
                } else {
                    out.push(line);
                }
            } else {
                out.push(line);
            }
        });
        return out;
    }

    public fromText(text: string) {
        const lines = this.splitIntoLines(text);

        const root: any = {};
        root.Plan = null;
        type ElementAtDepth = [number, any];
        // Array to keep reference to previous nodes with there depth
        const elementsAtDepth: ElementAtDepth[] = [];

        _.each(lines, (line: string) => {
            // Remove any trailing "
            line = line.replace(/"\s*$/, '');
            // Remove any begining "
            line = line.replace(/^\s*"/, '');
            // Replace tabs with 4 spaces
            line = line.replace(/\t/gm, '    ');

            const indentationRegex = /^\s*/;
            const depth = line.match(indentationRegex)![0].length;
            // remove indentation
            line = line.replace(indentationRegex, '');

            const emptyLineRegex = '^\s*$';
            const headerRegex = '^\\s*(QUERY|---|#).*$';
            const prefixRegex = '^(\\s*->\\s*|\\s*)';
            const typeRegex = '([^\\r\\n\\t\\f\\v\\:\\(]*?)';
            // tslint:disable-next-line:max-line-length
            const estimationRegex = '\\(cost=(\\d+\\.\\d+)\\.\\.(\\d+\\.\\d+)\\s+rows=(\\d+)\\s+width=(\\d+)\\)';
            const nonCapturingGroupOpen = '(?:';
            const nonCapturingGroupClose = ')';
            const openParenthesisRegex = '\\(';
            const closeParenthesisRegex = '\\)';
            // tslint:disable-next-line:max-line-length
            const actualRegex = '(?:actual\\stime=(\\d+\\.\\d+)\\.\\.(\\d+\\.\\d+)\\srows=(\\d+)\\sloops=(\\d+)|actual\\srows=(\\d+)\\sloops=(\\d+)|(never\\s+executed))';
            const optionalGroup = '?';

            const emptyLineMatches = new RegExp(emptyLineRegex).exec(line);
            const headerMatches = new RegExp(headerRegex).exec(line);

            /*
             * Groups
             * 1: prefix
             * 2: type
             * 3: estimated_startup_cost
             * 4: estimated_total_cost
             * 5: estimated_rows
             * 6: estimated_row_width
             * 7: actual_time_first
             * 8: actual_time_last
             * 9: actual_rows
             * 10: actual_loops
             * 11: actual_rows_
             * 12: actual_loops_
             * 13: never_executed
             * 14: estimated_startup_cost
             * 15: estimated_total_cost
             * 16: estimated_rows
             * 17: estimated_row_width
             * 18: actual_time_first
             * 19: actual_time_last
             * 20: actual_rows
             * 21: actual_loops
             */
            const nodeRegex = new RegExp(
                prefixRegex +
                typeRegex +
                '\\s*' +
                nonCapturingGroupOpen +
                (nonCapturingGroupOpen + estimationRegex + '\\s+' +
                    openParenthesisRegex + actualRegex + closeParenthesisRegex +
                    nonCapturingGroupClose) +
                '|' +
                nonCapturingGroupOpen + estimationRegex + nonCapturingGroupClose +
                '|' +
                nonCapturingGroupOpen + openParenthesisRegex + actualRegex +
                closeParenthesisRegex + nonCapturingGroupClose +
                nonCapturingGroupClose +
                '\\s*$',
                'gm',
            );
            const nodeMatches = nodeRegex.exec(line);

            // tslint:disable-next-line:max-line-length
            const subRegex = /^(\s*)((?:Sub|Init)Plan)\s*(?:\d+\s*)?\s*(?:\(returns.*\)\s*)?$/gm;
            const subMatches = subRegex.exec(line);

            const cteRegex = /^(\s*)CTE\s+(\S+)\s*$/g;
            const cteMatches = cteRegex.exec(line);

            /*
             * Groups
             * 2: trigger name
             * 3: time
             * 4: calls
             */
            const triggerRegex = /^(\s*)Trigger\s+(.*):\s+time=(\d+\.\d+)\s+calls=(\d+)\s*$/g;
            const triggerMatches = triggerRegex.exec(line);

            /*
             * Groups
             * 2: Worker number
             * 3: actual_time_first
             * 4: actual_time_last
             * 5: actual_rows
             * 6: actual_loops
             * 7: actual_rows_
             * 8: actual_loops_
             * 9: never_executed
             * 10: extra
             */
            const workerRegex = new RegExp(
                /^(\s*)Worker\s+(\d+):\s+/.source +
                nonCapturingGroupOpen +
                actualRegex +
                nonCapturingGroupClose +
                optionalGroup +
                '(.*)' +
                '\\s*$',
                'g',
            );
            const workerMatches = workerRegex.exec(line);

            const jitRegex = /^(\s*)JIT:\s*$/g;
            const jitMatches = jitRegex.exec(line);

            const extraRegex = /^(\s*)(\S.*\S)\s*$/g;
            const extraMatches = extraRegex.exec(line);

            if (emptyLineMatches || headerMatches) {
                return;
            } else if (nodeMatches && !cteMatches && !subMatches) {
                const prefix = nodeMatches[1];
                const neverExecuted = nodeMatches[13];
                const newNode: Node = new Node(nodeMatches[2]);
                if (nodeMatches[3] && nodeMatches[4] || nodeMatches[14] && nodeMatches[15]) {
                    newNode[NodeProp.STARTUP_COST] = parseFloat(nodeMatches[3] || nodeMatches[14]);
                    newNode[NodeProp.TOTAL_COST] = parseFloat(nodeMatches[4] || nodeMatches[15]);
                    newNode[NodeProp.PLAN_ROWS] = parseInt(nodeMatches[5] || nodeMatches[16], 0);
                    newNode[NodeProp.PLAN_WIDTH] = parseInt(nodeMatches[6] || nodeMatches[17], 0);
                }
                if (nodeMatches[7] && nodeMatches[8] || nodeMatches[18] && nodeMatches[19]) {
                    newNode[NodeProp.ACTUAL_STARTUP_TIME] = parseFloat(nodeMatches[7] || nodeMatches[18]);
                    newNode[NodeProp.ACTUAL_TOTAL_TIME] = parseFloat(nodeMatches[8] || nodeMatches[19]);
                }

                if (nodeMatches[9] && nodeMatches[10] || nodeMatches[11] && nodeMatches[12] ||
                    nodeMatches[20] && nodeMatches[21]) {
                    newNode[NodeProp.ACTUAL_ROWS] = parseInt(nodeMatches[9] || nodeMatches[11] || nodeMatches[20], 0);
                    newNode[NodeProp.ACTUAL_LOOPS] = parseInt(nodeMatches[10] || nodeMatches[12] || nodeMatches[21], 0);
                }

                if (neverExecuted) {
                    newNode[NodeProp.ACTUAL_LOOPS] = 0;
                    newNode[NodeProp.ACTUAL_ROWS] = 0;
                    newNode[NodeProp.ACTUAL_TOTAL_TIME] = 0;
                }
                const element = {
                    node: newNode,
                    subelementType: 'subnode',
                };

                if (0 === elementsAtDepth.length) {
                    elementsAtDepth.push([depth, element]);
                    root.Plan = newNode;
                    return;
                }

                // Remove elements from elementsAtDepth for deeper levels
                _.remove(elementsAtDepth, (e) => {
                    return e[0] >= depth;
                });

                // ! is for non-null assertion
                // Prevents the "Object is possibly 'undefined'" linting error
                const previousElement = _.last(elementsAtDepth)![1];

                elementsAtDepth.push([depth, element]);

                if (!previousElement.node[NodeProp.PLANS]) {
                    previousElement.node[NodeProp.PLANS] = [];
                }
                if (previousElement.subelementType === 'initplan') {
                    newNode[NodeProp.PARENT_RELATIONSHIP] = 'InitPlan';
                    newNode[NodeProp.SUBPLAN_NAME] = previousElement.name;
                } else if (previousElement.subelementType === 'subplan') {
                    newNode[NodeProp.PARENT_RELATIONSHIP] = 'SubPlan';
                    newNode[NodeProp.SUBPLAN_NAME] = previousElement.name;
                }
                previousElement.node.Plans.push(newNode);

            } else if (subMatches) {
                const prefix = subMatches[1];
                const type = subMatches[2];
                // Remove elements from elementsAtDepth for deeper levels
                _.remove(elementsAtDepth, (e) => e[0] >= depth);
                const previousElement = _.last(elementsAtDepth)![1];
                const element = {
                    node: previousElement.node,
                    subelementType: type.toLowerCase(),
                    name: subMatches[0],
                };
                elementsAtDepth.push([depth, element]);
            } else if (cteMatches) {
                const prefix = cteMatches[1];
                const cteName = cteMatches[2];
                // Remove elements from elementsAtDepth for deeper levels
                _.remove(elementsAtDepth, (e) => e[0] >= depth);
                const previousElement = _.last(elementsAtDepth)![1];
                const element = {
                    node: previousElement.node,
                    subelementType: 'initplan',
                    name: 'CTE ' + cteName,
                };
                elementsAtDepth.push([depth, element]);
            } else if (workerMatches) {
                const prefix = workerMatches[1];
                const workerNumber = parseInt(workerMatches[2], 0);
                const previousElement = _.last(elementsAtDepth)![1];
                if (!previousElement.node[NodeProp.WORKERS]) {
                    previousElement.node[NodeProp.WORKERS] = [];
                }
                let worker = this.getWorker(previousElement.node, workerNumber);
                if (!worker) {
                    worker = new Worker(workerNumber);
                    previousElement.node[NodeProp.WORKERS].push(worker);
                }
                if (workerMatches[3] && workerMatches[4]) {
                    worker[NodeProp.ACTUAL_STARTUP_TIME] = parseFloat(workerMatches[3]);
                    worker[NodeProp.ACTUAL_TOTAL_TIME] = parseFloat(workerMatches[4]);
                    worker[NodeProp.ACTUAL_ROWS] = parseInt(workerMatches[5], 0);
                    worker[NodeProp.ACTUAL_LOOPS] = parseInt(workerMatches[6], 0);
                }

                if (this.parseSort(workerMatches[10], worker)) {
                    return;
                }

                // extra info
                const info = workerMatches[10].split(/: (.+)/).filter((x) => x);
                if (workerMatches[10]) {
                    if (!info[1]) {
                        return;
                    }
                    const property = _.startCase(info[0]);
                    worker[property] = info[1];
                }
            } else if (triggerMatches) {
                const prefix = triggerMatches[1];
                // Remove elements from elementsAtDepth for deeper levels
                _.remove(elementsAtDepth, (e) => e[0] >= depth);
                root.Triggers = root.Triggers || [];
                root.Triggers.push({
                    'Trigger Name': triggerMatches[2],
                    'Time': this.parseTime(triggerMatches[3]),
                    'Calls': triggerMatches[4],
                });
            } else if (jitMatches) {
                let element;
                if (elementsAtDepth.length === 0) {
                    root.JIT = {};
                    element = {
                        node: root.JIT,
                    };
                    elementsAtDepth.push([1, element]);
                } else {
                    const lastElement = _.last(elementsAtDepth)![1];
                    if (_.last(lastElement.node[NodeProp.WORKERS])) {
                        const worker: Worker = _.last(lastElement.node[NodeProp.WORKERS])! as Worker;
                        worker.JIT = {};
                        element = {
                            node: worker.JIT,
                        };
                        elementsAtDepth.push([depth, element]);
                    }
                }
            } else if (extraMatches) {
                const prefix = extraMatches[1];

                // Remove elements from elementsAtDepth for deeper levels
                _.remove(elementsAtDepth, (e) => e[0] >= depth);

                let element;
                if (elementsAtDepth.length === 0) {
                    element = root;
                } else {
                    element = _.last(elementsAtDepth)![1].node;
                }

                // if no node have been found yet and a 'Query Text' has been found
                // there the line is the part of the query
                if (!element.Plan && element['Query Text']) {
                    element['Query Text'] += '\n' + line;
                    return;
                }

                const info = extraMatches[2].split(/: (.+)/).filter((x) => x);
                if (!info[1]) {
                    return;
                }

                if (this.parseSort(extraMatches[2], element)) {
                    return;
                }

                if (this.parseBuffers(extraMatches[2], element)) {
                    return;
                }

                if (this.parseWAL(extraMatches[2], element)) {
                    return;
                }

                if (this.parseIOTimings(extraMatches[2], element)) {
                    return;
                }

                if (this.parseOptions(extraMatches[2], element)) {
                    return;
                }

                if (this.parseTiming(extraMatches[2], element)) {
                    return;
                }

                if (this.parseSettings(extraMatches[2], element)) {
                    return;
                }

                if (this.parseSortGroups(extraMatches[2], element)) {
                    return;
                }

                if (this.parseSortKey(extraMatches[2], element)) {
                    return;
                }

                // remove the " ms" unit in case of time
                let value: string | number = info[1].replace(/(\s*ms)$/, '');
                // try to convert to number
                if (parseFloat(value)) {
                    value = parseFloat(value);
                }

                let property = info[0];
                if (property.indexOf(' runtime') !== -1 || property.indexOf(' time') !== -1) {
                    property = _.startCase(property);
                }
                element[property] = value;
            }
        });
        if (!root.Plan) {
            throw new Error('Unable to parse plan');
        }
        return root;
    }
    private parseTime(text: string): number {
        return parseFloat(text.replace(/(\s*ms)$/, ''));
    }

    private parseSettings(text: string, el: Node): boolean {
        // Parses a settings block
        // eg. Timing: Generation 0.340 ms, Inlining 0.000 ms, Optimization 0.168 ms, Emission 1.907 ms, Total 2.414 ms

        const settingsRegex = /^(\s*)Settings:\s*(.*)$/g;
        const settingsMatches = settingsRegex.exec(text);

        if (settingsMatches) {
            el.Settings = {};
            const settings = splitBalanced(settingsMatches[2], ',');
            let matches;
            _.each(settings, (option) => {
                const reg = /^(\S*)\s+=\s+(.*)$/g;
                matches = reg.exec(_.trim(option));
                el.Settings[matches![1]] = matches![2].replace(/'/g, '');
            });
            return true;
        }

        return false;
    }

    private parseSortGroups(text: string, el: Node): boolean {
        // Parses a Full-sort Groups block
        // eg. Full-sort Groups: 312500  Sort Method: quicksort  Average Memory: 26kB  Peak Memory: 26kB
        const sortGroupsRegex = /^\s*(Full-sort|Pre-sorted) Groups:\s+([0-9]*)\s+Sort Method[s]*:\s+(.*)\s+Average Memory:\s+(\S*)kB\s+Peak Memory:\s+(\S*)kB.*$/g;
        const matches = sortGroupsRegex.exec(text);

        if (matches) {
            const groups: { [key in SortGroupsProp]: any } = {
                [SortGroupsProp.GROUP_COUNT]: parseInt(matches[2], 0),
                [SortGroupsProp.SORT_METHODS_USED]: _.map(matches[3].split(','), _.trim),
                [SortGroupsProp.SORT_SPACE_MEMORY]: {
                    [SortSpaceMemory.AVERAGE_SORT_SPACE_USED]: parseInt(matches[4], 0),
                    [SortSpaceMemory.PEAK_SORT_SPACE_USED]: parseInt(matches[5], 0),
                },
            };

            if (matches[1] === 'Full-sort') {
                el[NodeProp.FULL_SORT_GROUPS] = groups;
            } else if (matches[1] === 'Pre-sorted') {
                el[NodeProp.PRE_SORTED_GROUPS] = groups;
            } else {
                throw new Error('Unsupported sort groups method');
            }
            return true;
        }
        return false;
    }

    private parseSortKey(text: string, el: Node): boolean {
        const sortRegex = /^\s*((?:Sort|Presorted) Key):\s+(.*)/g;
        const sortMatches = sortRegex.exec(text);
        if (sortMatches) {
            el[sortMatches[1]] = _.map(splitBalanced(sortMatches[2], ','), _.trim);
            return true;
        }
        return false;
    }

    private parseSort(text: string, el: Node | Worker): boolean {
        /*
         * Groups
         * 2: Sort Method
         * 3: Sort Space Type
         * 4: Sort Space Used
         */
        const sortRegex = /^(\s*)Sort Method:\s+(.*)\s+(Memory|Disk):\s+(?:(\S*)kB)\s*$/g;
        const sortMatches = sortRegex.exec(text);
        if (sortMatches) {
            el[NodeProp.SORT_METHOD] = sortMatches[2].trim();
            el[NodeProp.SORT_SPACE_USED] = sortMatches[4];
            el[NodeProp.SORT_SPACE_TYPE] = sortMatches[3];
            return true;
        }
        return false;
    }

    private parseBuffers(text: string, el: Node | Worker): boolean {
        /*
         * Groups
         */
        const buffersRegex = /Buffers:\s+(.*)\s*$/g;
        const buffersMatches = buffersRegex.exec(text);

        /*
         * Groups:
         * 1: type
         * 2: info
         */
        if (buffersMatches) {
            _.each(buffersMatches[1].split(/,\s+/), (infos) => {
                const bufferInfoRegex = /(shared|temp|local)\s+(.*)$/g;
                const m = bufferInfoRegex.exec(infos);
                if (m) {
                    const type = m[1];
                    // Initiate with default value
                    _.each(['hit', 'read', 'written', 'dirtied'], (method) => {
                        el[_.map([type, method, 'blocks'], _.capitalize).join(' ')] = 0;
                    });
                    _.each(m[2].split(/\s+/), (buffer) => {
                        this.parseBuffer(buffer, type, el);
                    });
                }
            });
            return true;
        }
        return false;
    }

    private parseBuffer(text: string, type: string, el: Node | Worker): void {
        const s = text.split(/=/);
        const method = s[0];
        const value = parseInt(s[1], 0);
        el[_.map([type, method, 'blocks'], _.capitalize).join(' ')] = value;
    }

    private getWorker(node: Node, workerNumber: number): Worker | null {
        return _.find(node[NodeProp.WORKERS], (worker) => {
            return worker[WorkerProp.WORKER_NUMBER] === workerNumber;
        });
    }

    private parseWAL(text: string, el: Node): boolean {
        const WALRegex = /WAL:\s+(.*)\s*$/g;
        const WALMatches = WALRegex.exec(text);

        if (WALMatches) {
            // Initiate with default value
            _.each(['Records', 'Bytes', 'FPI'], (type) => {
                el['WAL ' + type] = 0;
            });
            _.each(WALMatches[1].split(/\s+/), (t) => {
                const s = t.split(/=/);
                const type = s[0];
                const value = parseInt(s[1], 0);
                let typeCaps;
                switch (type) {
                    case 'fpi':
                        typeCaps = 'FPI';
                        break;
                    default:
                        typeCaps = _.capitalize(type);
                }
                el['WAL ' + typeCaps] = value;
            });
            return true;
        }

        return false;
    }

    private parseIOTimings(text: string, el: Node): boolean {
        /*
         * Groups
         */
        const iotimingsRegex = /I\/O Timings:\s+(.*)\s*$/g;
        const iotimingsMatches = iotimingsRegex.exec(text);

        /*
         * Groups:
         * 1: type
         * 2: info
         */
        if (iotimingsMatches) {
            // Initiate with default value
            el[NodeProp.IO_READ_TIME] = 0;
            el[NodeProp.IO_WRITE_TIME] = 0;

            _.each(iotimingsMatches[1].split(/\s+/), (timing) => {
                const s = timing.split(/=/);
                const method = s[0];
                const value = parseFloat(s[1]);
                const prop = 'IO_' + _.upperCase(method) + '_TIME' as keyof typeof NodeProp;
                el[NodeProp[prop]] = value;
            });
            return true;
        }
        return false;
    }

    private parseOptions(text: string, el: Node): boolean {
        // Parses an options block in JIT block
        // eg. Options: Inlining false, Optimization false, Expressions true, Deforming true

        /*
         * Groups
         */
        const optionsRegex = /^(\s*)Options:\s+(.*)$/g;
        const optionsMatches = optionsRegex.exec(text);

        if (optionsMatches) {
            el.Options = {};
            const options = optionsMatches[2].split(/\s*,\s*/);
            let matches;
            _.each(options, (option) => {
                const reg = /^(\S*)\s+(.*)$/g;
                matches = reg.exec(option);
                el.Options[matches![1]] = JSON.parse(matches![2]);
            });
            return true;
        }
        return false;
    }

    private parseTiming(text: string, el: Node): boolean {
        // Parses a timing block in JIT block
        // eg. Timing: Generation 0.340 ms, Inlining 0.000 ms, Optimization 0.168 ms, Emission 1.907 ms, Total 2.414 ms

        /*
         * Groups
         */
        const timingRegex = /^(\s*)Timing:\s+(.*)$/g;
        const timingMatches = timingRegex.exec(text);

        if (timingMatches) {
            el.Timing = {};
            const timings = timingMatches[2].split(/\s*,\s*/);
            let matches;
            _.each(timings, (option) => {
                const reg = /^(\S*)\s+(.*)$/g;
                matches = reg.exec(option);
                el.Timing[matches![1]] = this.parseTime(matches![2]);
            });
            return true;
        }
        return false;
    }
}
