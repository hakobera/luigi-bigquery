SELECT
  count(*) cnt
FROM
  [publicdata:samples.github_nested]
WHERE
  repository.language = '{{ language }}'
