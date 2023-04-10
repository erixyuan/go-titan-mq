import axios from 'axios';
import qs from "query-string";

export interface MessageRecord {
  id: number;
  type: string;
  title: string;
  subTitle: string;
  avatar?: string;
  content: string;
  time: string;
  status: 0 | 1;
  messageType?: number;
}
export type MessageListType = MessageRecord[];

export interface MessageListRes {
  list: MessageRecord[];
  total: number;
}

export interface MessageParams extends Partial<MessageRecord> {
  topic:string,
  queueId:number,
  current: number;
  pageSize: number;
}


export function queryMessageList(params: MessageParams) {
  const customConfig = {
    headers: {
      'Content-Type': 'application/json'
    }
  };
  return axios.post<MessageListRes>('/message/list', JSON.stringify(params), customConfig);
}

interface MessageStatus {
  ids: number[];
}

export function setMessageStatus(data: MessageStatus) {
  return axios.post<MessageListType>('/message/read', data);
}

export interface ChatRecord {
  id: number;
  username: string;
  content: string;
  time: string;
  isCollect: boolean;
}

export function queryChatList() {
  return axios.post<ChatRecord[]>('/api/chat/list');
}
