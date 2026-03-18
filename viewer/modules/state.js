'use strict';

(function initViewerState(globalObj) {
  function createState() {
    return {
      sourcesMeta: [],
      allProducts: [],
      allData: {},
      dbProducts: [],
      baseModels: [],
      standardGroups: [],
      storageSizes: [],
      subitoAds: [],
      subitoOpportunities: [],
      ebaySold: [],
      currentSort: { key: 'last_price', dir: 1 },
    };
  }

  globalObj.ViewerState = createState();
})(window);
