// add code format for arc types
require(['notebook/js/codecell'], function (codecell) {
  codecell.CodeCell.options_default.highlight_modes['magic_text/x-sh'] = { 'reg': [/^(%env|%metadata)/] };
  codecell.CodeCell.options_default.highlight_modes['magic_text/x-sql'] = { 'reg': [/^%sql/] };
  codecell.CodeCell.options_default.highlight_modes['magic_application/x-cypher-query'] = { 'reg': [/^%cypher/] };
  codecell.CodeCell.options_default.highlight_modes['magic_text/javascript'] = { 'reg': [/^%arc|^{/] };

  // auto highlight on start
  Jupyter.notebook.events.on('kernel_ready.Kernel', function () {
    Jupyter.notebook.get_cells().map(function (cell) {
      if (cell.cell_type == 'code') { cell.auto_highlight(); }
    });
  });

});

