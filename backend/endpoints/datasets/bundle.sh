pip install --requirement=datasets/requirements.txt --target=/asset-output \
&& \
mkdir --parents /asset-output/endpoints/datasets \
&& \
touch {/asset-output/endpoints/__init__.py,/asset-output/endpoints/datasets/__init__.py} \
&& \
cp --archive --update --verbose datasets/*.py /asset-output/endpoints/datasets/ \
&& \
cp --archive --update --verbose utils.py /asset-output/endpoints/
