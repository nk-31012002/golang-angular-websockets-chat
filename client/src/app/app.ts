import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subscription } from 'rxjs';
import { SocketService, ChatEvent } from './socket';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App implements OnInit, OnDestroy {
  public messages: string[] = [];
  public chatBox: string = '';
  private socketSub!: Subscription;

  constructor(private socket: SocketService) {}

  ngOnInit(): void {
    this.socketSub = this.socket.getEvents().subscribe((event: ChatEvent) => {
      if (event.type === 'message') {
        let display = event.data.content || '';
        if (event.data.sender) {
          const shortSender = event.data.sender.substring(0, 8);
          display = `${shortSender}: ${display}`;
        }
        this.messages.push(display);
      } else if (event.type === 'open') {
        this.messages.push('/The socket connection has been established');
      } else if (event.type === 'close') {
        this.messages.push('/The socket connection has been closed');
      }
    });
  }

  ngOnDestroy(): void {
    if (this.socketSub) {
      this.socketSub.unsubscribe();
    }
    this.socket.close();
  }

  public send(): void {
    const trimmed = this.chatBox.trim();
    if (trimmed) {
      this.socket.send(trimmed);
      this.chatBox = '';
    }
  }

  public isSystemMessage(message: string): boolean {
    return message.startsWith('/');
  }

  public formatMessage(message: string): string {
    return message.startsWith('/') ? message.substring(1) : message;
  }
}