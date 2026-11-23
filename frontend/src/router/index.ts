import { createRouter, createWebHistory } from 'vue-router'

import BatchIngestView from '@/views/BatchIngestView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/batch_ingest', name: 'batch_ingest', component: BatchIngestView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
