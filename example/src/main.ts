import Vue from 'vue';
import 'bootstrap';
import 'bootstrap/dist/css/bootstrap.min.css';
import '@fortawesome/fontawesome-free/css/all.css';
import Home from './views/Home.vue';
import Plan from './views/Plan.vue';
import About from './views/About.vue';

import tidbData from './join-tidb.json';

const routes = {
  '/': Home,
  '/plan': Plan,
  '/about': About,
};

const data = JSON.stringify(tidbData)

export const planData: any[] = [data, ''];

Vue.config.productionTip = false;

export function setPlanData(plan, query) {
  planData[0] = plan;
  planData[1] = query;
  app.currentRoute = '/plan';
}

global.setPlanData = setPlanData;

const app = new Vue({
  data: {
    currentRoute: '/plan',
  },
  computed: {
    ViewComponent() {
      return routes[this.currentRoute];
    },
  },
  render(h) {
    return h(this.ViewComponent);
  },
}).$mount('#app');
