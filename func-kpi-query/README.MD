git checkout dev
git config --global merge.ours.driver true
git checkout master

## Não é necessário executar este passo. Arquivo .gitattibutes já está criado
## echo "path/connection.py. merge=ours" >> .gitattributes

git merge dev