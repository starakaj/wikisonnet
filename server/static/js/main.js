// custom scripts
$(function() {

  $("#search").autocomplete({
    minLength: 3,
    source: function(request, response) {
        $.ajax({
          url: "/search",
          type: "GET",
          data: {
            q: request.term
          },
          success: function(results) {
            response(results.list);
          }
        })
    },
    select: function(event, ui) {
      $("#submit-button").prop("disabled", false);
    }
  });
});