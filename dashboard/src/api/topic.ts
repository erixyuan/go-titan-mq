import axios from 'axios';

export interface TopicRecord {
    name: string;
    queueIds : number[];
}

export interface TopicListRes {
    list : TopicRecord[]
}

export function queryTopicList(){
    const customConfig = {
        headers: {
            'Content-Type': 'application/json'
        }
    };
    return axios.post<TopicListRes>('/topic/list', {}, customConfig);
}
