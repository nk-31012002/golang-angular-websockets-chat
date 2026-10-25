import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';

export interface ChatEvent {
  type: 'open' | 'close' | 'message';
  data?: any;
}

@Injectable({
  providedIn: 'root'
})
export class SocketService {
  private socket!: WebSocket;
  private eventSubject: Subject<ChatEvent> = new Subject<ChatEvent>();

  constructor() {
    this.connect();
  }

  private connect(): void {
    this.socket = new WebSocket('ws://localhost:12345/ws');

    this.socket.onopen = (event) => {
      this.eventSubject.next({ type: 'open', data: event });
    };

    this.socket.onclose = (event) => {
      this.eventSubject.next({ type: 'close', data: event });
    };

    this.socket.onmessage = (event) => {
      try {
        const parsed = JSON.parse(event.data);
        this.eventSubject.next({ type: 'message', data: parsed });
      } catch {
        this.eventSubject.next({ type: 'message', data: { content: event.data } });
      }
    };

    this.socket.onerror = (err) => {
      console.error('WebSocket Error:', err);
    };
  }

  public send(message: string): void {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(message);
    }
  }

  public close(): void {
    if (this.socket) {
      this.socket.close();
    }
  }

  public getEvents(): Observable<ChatEvent> {
    return this.eventSubject.asObservable();
  }
}