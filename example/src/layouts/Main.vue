<template>
  <div class="d-flex flex-column vh-100">
    <div class="navbar container flex-shrink-0">
     <h1>TiDB EXPLAIN Visualizer</h1>
    </div>
    <slot></slot>
  </div>
</template>

<script>
  import VLink from '../components/VLink.vue'
  export default {
    components: {
      VLink
    }
  }
</script>
