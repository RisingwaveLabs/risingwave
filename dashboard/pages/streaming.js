import Layout from '../components/Layout';
import StreamingView from '../components/StreamingView';
import NoData from '../components/NoData';
import { getActors } from './api/streaming';
import { cloneDeep } from "lodash";

export async function getStaticProps(context) {
  let actorProtoList = await getActors();
  actorProtoList.push(cloneDeep(actorProtoList[0]))  // TODO: remove this (for testing)
  return {
    props: {
      actorProtoList
    }
  }
}

export default function Streaming(props) {
  return (
    <>
      <Layout currentPage="streaming">
        {props.actorProtoList.length !== 0 ?
          props.actorProtoList.map((data, index) =>
            <StreamingView
              key={index}
              node={data.node}
              actorProto={data}
            />
          )
          : <NoData />}
      </Layout>
    </>
  )
}