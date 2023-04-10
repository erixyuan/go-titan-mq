import { DEFAULT_LAYOUT } from '../base';
import { AppRouteRecordRaw } from '../types';

const LIST: AppRouteRecordRaw = {
  path: '/message',
  name: 'message',
  component: DEFAULT_LAYOUT,
  meta: {
    locale: 'menu.message',
    requiresAuth: true,
    icon: 'icon-list',
    order: 2,
  },
  children: [
    {
      path: 'list', // The midline path complies with SEO specifications
      name: 'messageList',
      component: () => import('@/views/message/index.vue'),
      meta: {
        locale: '消息列表',
        requiresAuth: true,
        roles: ['*'],
      },
    },
  ],
};

export default LIST;
