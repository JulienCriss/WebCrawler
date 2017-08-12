$(function() {
  $("#searchInput").autocomplete({
    source: "/api/search/",
    minLength: 2,
  });
});