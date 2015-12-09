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
            results.list.forEach(function(element) {
              if (request.term == element) {
                $("#submit-button").prop("disabled", false);
              }
            });
            response(results.list);
          }
        })
    },
    select: function(event, ui) {
      $("#submit-button").prop("disabled", false);
    }
  });
});