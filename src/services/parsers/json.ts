/**
 * A parser to parse JSON-format plan data.
 */

import * as _ from 'lodash';
import clarinet from 'clarinet';

export class JSONParser {
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
}
