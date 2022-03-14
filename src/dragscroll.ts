/**
 * Handle drag and scroll events on plan viewport.
 */

export default class Dragscroll {
  private element: Element;
  private listener: boolean;
  private start: boolean;
  private startMousePositionX: number;
  private startMousePositionY: number;
  private startScrollPositionX: number;
  private startScrollPositionY: number;

  constructor(element: Element) {
    this.element = element;
    this.listener = true;

    this.start = false;
    this.startMousePositionX = 0;
    this.startMousePositionY = 0;
    this.startScrollPositionX = 0;
    this.startScrollPositionY = 0;

    this.element.addEventListener('mousedown', this.handleMouseDownOnPlan.bind(this));
    this.element.addEventListener('mousemove', this.handleMouseMoveOnPlan.bind(this));
    document.documentElement.addEventListener('mouseup', this.handleMouseUpOnDocument.bind(this));
  }

  private handleMouseDownOnPlan(e: any) {
    if (e.target.closest('.plan-node-body')) {
      return;
    }

    e.preventDefault();
    this.clearSelection();

    this.startMousePositionX = e.screenX;
    this.startMousePositionY = e.screenY;
    this.startScrollPositionX = this.element.scrollLeft;
    this.startScrollPositionY = this.element.scrollTop;

    this.start = true;
  }

  private handleMouseUpOnDocument(e: any) {
    e.preventDefault();

    this.startMousePositionX = 0;
    this.startMousePositionY = 0;
    this.startScrollPositionX = 0;
    this.startScrollPositionY = 0;

    this.start = false;
  }

  private handleMouseMoveOnPlan(e: any) {
    if (this.listener && this.start) {
      e.preventDefault();

      this.element.scrollTo(
        this.startScrollPositionX + (this.startMousePositionX - e.screenX),
        this.startScrollPositionY + (this.startMousePositionY - e.screenY),
      );
    }
  }

  private clearSelection() {
    if (window.getSelection) {
      const sel = window.getSelection();
      if (sel) {
        sel.removeAllRanges();
      }
    }
  }
}
